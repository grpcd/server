# grpcd server

The grpcd server. It answers the three RPCs in
[grpcd/protos](https://github.com/grpcd/protos), holds the streams that are
registrations, and keeps the rows in a storage backend shared by every instance
in a region.

It is a [connect-service](https://github.com/pbrpc/connect-service) server: one
port serves gRPC, gRPC-Web, and Connect, and `GET /healthz` answers plain HTTP
probes. A registration lasts as long as the stream holding it. The server ends
no stream of its own accord, so a service stays registered until it leaves or
its connection breaks.

Container images are published to `ghcr.io/grpcd/server`.

## Quick Start

```bash
# In-memory storage (development)
docker run -p 50051:50051 ghcr.io/grpcd/server:latest

# Redis (production)
docker run -p 50051:50051 \
  -e STORAGE_BACKEND=redis \
  -e STORAGE_ADDRESS=redis:6379 \
  ghcr.io/grpcd/server:latest
```

## Configuration

Everything is an environment variable.

| Variable              | Description                                                            | Default  |
| --------------------- | ---------------------------------------------------------------------- | -------- |
| `SERVICE_ADDRESS`     | Address to bind                                                        | `:50051` |
| `TLS_CERT`, `TLS_KEY` | The listener's certificate and key as PEM. Unset is cleartext.         | -        |
| `TLS_CLIENT_CA`       | A PEM CA. When set, every client must present a certificate it signed. | -        |
| `STORAGE_BACKEND`     | `redis`, or empty for in-memory                                        | -        |
| `STORAGE_ADDRESS`     | Storage backend address. Required for `redis`.                         | -        |

Each `TLS_*` variable holds the material itself, not a path to it.
`SERVICE_NAME`, `SERVICE_VERSION`, `MAX_CONNECTION_IDLE`,
`HTTP_SERVER_IDLE_TIMEOUT`, `HTTP2_SEND_PING_TIMEOUT`, `HTTP2_PING_TIMEOUT`,
`OTEL_*`, and `LOG_FORMAT` are read as on every service across the
[`pbrpc` ecosystem](https://github.com/pbrpc).

### Health

`GET /healthz` answers for the process: `200` with `{"status":"SERVING"}` while
it is up. `GET /healthz?service=grpcd.GRPCDService` answers for the service,
which is whether the store can be reached: `503` with `{"status":"NOT_SERVING"}`
from the moment the store's subscription breaks, or an operation fails against
the store, until the subscription is back or an operation succeeds, and `200`
with `SERVING` otherwise.

A balancer or orchestrator probe that should route around an instance that has
lost its store asks for `grpcd.GRPCDService`. A probe that asks for the process
sees only whether it is up.

## Design

### State lives in the storage backend

Every fact the server serves is in the storage backend, so any instance answers
any lookup and a replacement instance serves immediately. An instance
additionally holds the registration streams it accepted, which is what ties a
row's lifetime to its service's.

```
┌────────────────────────────────────────┐
│              grpcd server              │
│                                        │
│   ┌──────────────────────────────┐     │
│   │     Service Layer            │     │
│   │  • Register (held stream)    │     │
│   │  • Discover (bidirectional)  │     │
│   │  • Watch (held stream)       │     │
│   │  • Validation                │     │
│   └──────────┬───────────────────┘     │
│              │                         │
│   ┌──────────▼───────────────────┐     │
│   │   Storage Interface          │     │
│   └──────────┬───────────────────┘     │
│              │                         │
└──────────────┼─────────────────────────┘
               │
     ┌─────────┴─────────┐
     │                   │
 ┌───▼────┐      ┌───────▼────┐
 │ Redis  │      │    Mock    │
 │Backend │      │  (Testing) │
 └────────┘      └────────────┘
```

The backend owns persistence and replication. Embedded storage would need Raft
for HA; in-memory with peer sync would need gossip and reconciliation; a direct
Redis dependency would be untestable without Redis. The interface keeps the
server to the business logic and leaves storage HA to whoever runs the store.

### Data model

Two mappings, neither with an expiry:

- **method → addresses.** Key: the method name. Value: the set of addresses.
  Read by `Discover`; its cardinality is the `N` a `Watch` draws against.
- **address → anchor.** Key: the address. Value: the id of the instance holding
  that address's `Register` stream. Read to address the notification when a row
  is removed.

An instance generates a UUIDv4 at startup and uses it as its anchor id and as
its notification channel. The id names a channel that lives and dies with the
process, so it needs no coordination and no durability.

### Registration

`internal/service/register.go`. The handler validates the request, reads the IP
off the call's peer address, composes the address, writes one row per method
plus the anchor, sends the acknowledgement, and blocks on the stream. When the
stream ends it removes every row the request named. The handler holds the
request for the life of the stream, and writes the registration again from it
whenever the rows may be gone; no copy of what any stream registered is kept
anywhere else. The instance keeps only which handlers hold a stream at which
address, to hand a removal to.

### Discovery

`internal/service/discover.go`. `AddressesFor` on the store is a sequence, and
each pull is one uniform random draw over the set as it is at that moment, so a
lookup costs O(1) per candidate however many addresses the method has and a
removed address is never drawn again. The sequence ends when the set is empty,
and the handler then waits for the next addition announced for the method.

### Rebalancing

`internal/service/watch.go`. The handler sleeps until an addition is announced.
On one for its method whose address differs from the one held, it draws with
probability `1/N` and sends the new address only if it wins. Two additions back
to back can cost one missed rebalance, which the next addition corrects; the
store's set is the truth throughout.

### Notifications

Every registration is announced once per instance, over one subscription to the
storage backend, and every handler waiting on that instance is woken by that one
announcement. Each woken handler reads the store to learn whether the
registration concerned it. No instance holds a list of who is waiting for what.

Instances also notify each other about removals, over the backend's
publish/subscribe, addressed to a single anchor.

### Reverting a wrong removal

The instance anchoring an address is notified when a row for it is removed, and
hands the removal to the `Register` handler holding a stream at that address.
The handler writes its registration again from the request it holds: its open
stream is live proof the service is up, so it performs no check of its own. It
writes what the request names and nothing else, so a row left on the address by
an earlier occupant, one a container gave up and another took, stays removed. A
removal at an address no stream holds any more is a stream that ended in the
meantime, and it stands. `grpcd.removals.reverted.total` counts the
registrations written again, so a client with a persistent local fault surfaces
in monitoring.

### Losing the storage backend

`internal/service/outage.go`. An instance that cannot reach the backend can
neither record a registration nor answer a lookup, and is deaf to additions. It
keeps serving and reports `grpcd.GRPCDService` as `NOT_SERVING` until the
backend is back.

The loss is noticed two ways. The additions subscription is the one connection
to the backend that is always open, so a backend that dies is seen the moment
its socket closes, with nothing touching the store; a backend that vanishes
without closing it is seen when TCP keepalive gives up on the connection. An
operation that fails against the backend says the same thing, which is how a
backend that hangs without dropping its connections is found. The subscription
reconnects on a backoff schedule, resolving the address again each time, so a
backend brought back elsewhere under the same name is found; its resubscription
is the backend being back, as is any operation that succeeds against a store
recorded lost.

The streams it holds are kept. A handler that fails against the backend waits
for it to return and carries on: a registration is written once it can be, a
lookup draws again, a watch resumes. A client already on the instance sees a
call take longer and nothing else. A stream that ends during the outage has its
rows removed once, which fails, and they stay until a client fails against the
address and reports it; a removal applied later than the stream ended could
take rows a new occupant of the address has since written.

When the backend returns, every held registration writes its rows again from the
request the handler still holds, so a backend that came back empty is
repopulated by the instances themselves. Waiting lookups draw again, since the
backend may hold registrations the instance was deaf to.

## Failure Modes

**Service crash.** Its `Register` stream ends and its address is removed at
once. Clients holding a connection to it see that connection break and
rediscover.

**Instance crash.** Its rows remain, because removal happens when an instance
observes a stream ending and this one is gone; live services stay discoverable
throughout. Its registration streams break, and each service reconnects to
another instance and re-registers: the same rows, written again, with the anchor
overwritten by the new instance's id. Until that lands, those rows carry the id
of a channel nobody reads, so a wrong `dead_address` report in that window drops
a live service until it re-registers.

**Service and its anchoring instance crash together.** Nothing runs the removal,
so the row remains. The first client to discover that address fails against it
and reports it, which removes it.

**Storage backend failure.** Covered under Design. Services and clients continue
on the connections they already hold.

## Storage Backends

`internal/storage/store.go` is the interface, and `internal/storage/config.go`
the one configuration every backend is built from: which backend, and where it
is reached. A backend's constructor takes that configuration and refuses what
it cannot use, an empty address for one that reaches a process; the resolver in
`internal/storage/resolver/` parses it from the environment and switches on
the backend. Adding a backend means implementing the interface, naming it in
`Backend`, and adding a case to the resolver.

**In-memory** (`internal/storage/mock/`). Selected when `STORAGE_BACKEND` is
unset. No external dependency; state is lost on restart, and instances do not
share it.

**Redis** (`internal/storage/redis/`). The production backend. Sets hold the
method rows, and publish/subscribe carries the addition announcements and the
removal notifications. Instances share state, and Sentinel or Cluster supply HA.

## Observability

Counters, exported over OpenTelemetry:

- `grpcd.registrations.total`
- `grpcd.removals.total`
- `grpcd.discoveries.total`
- `grpcd.removals.reverted.total`
- `grpcd.rebalances.total`

The info and diagnostics services from
[connect-service](https://github.com/pbrpc/connect-service) report the version
and the store under the `storage` dependency: `REACHABLE` when it answers a
ping, `UNREACHABLE` with the failure under the `error` detail otherwise.

## Development

```bash
go vet ./...
go test -race -cover ./...
go test -fuzz=FuzzRegister_MethodNames ./internal/service
go test -fuzz=FuzzRegister_MethodCounts ./internal/service
go test -fuzz=FuzzDiscover_MethodNames ./internal/service
CGO_ENABLED=0 go build -ldflags="-w -s" -o grpcd .
```

The service tests drive the handlers through the generated client over the
in-process transport, so nothing listens. The transport hands messages over
unbuffered: a handler's `Send` returns once the test has received the message,
and its `Receive` once the test has sent or closed.
