//revive:disable:package-comments
package cli

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"connectrpc.com/connect/v2"
	"github.com/caarlos0/env/v11"
	"github.com/google/uuid"

	"github.com/grpcd/protos/grpcdconnect"
	connectserver "github.com/pbrpc/connect-server"
	"github.com/pbrpc/connect-service/diagnostics"
	"github.com/pbrpc/connect-service/health"
	service_lib "github.com/pbrpc/connect-service/service"
	"github.com/pbrpc/lifecycle"
	pbrpcotel "github.com/pbrpc/otel"
	svc "github.com/pbrpc/service"

	"github.com/grpcd/server/internal/service"
	"github.com/grpcd/server/internal/storage/resolver"
)

// cleanupTimeout bounds stopping the server and flushing telemetry, together
const cleanupTimeout = 5 * time.Second

// Run serves until a signal arrives or serving fails, and answers with the
// process exit code. Every return runs the deferred teardown on its way out.
func Run() int {
	// The process context. The shutdown builds its deadline on this one, which
	// is why the signal cancels a child of it rather than this.
	ctx := context.Background()

	serveCtx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	svcCfg := svc.Configuration{Name: "grpcd"}
	err := env.Parse(&svcCfg)
	if err != nil {
		slog.Default().Error("Failed to read configuration", slog.Any("error", err))
		return 1
	}

	stack := lifecycle.Stack{}

	serverName := svcCfg.Name

	// Registrations are streams this process holds, so what it anchors dies with
	// it. The id names a channel for the life of the process and is never
	// referenced afterward, so it needs no coordination and no durability. It
	// is also the instance id on every span, log line, and metric, so a row's
	// anchor and the telemetry of the process that wrote it carry one name.
	anchor := uuid.NewString()

	log, flush, err := pbrpcotel.Init(ctx, serverName, svcCfg.Version, anchor)
	if err != nil {
		slog.Default().Error("Failed to initialize telemetry", slog.Any("error", err))
		return 1
	}
	stack.Push(lifecycle.Logged(log, "telemetry", flush))

	host, err := connectserver.FromEnv(log)
	if err != nil {
		return 1
	}
	stack.Push(lifecycle.Logged(log, "server", host.HTTPHost.Server.Shutdown))

	// Deferred before anything else can fail, so every path out of here stops
	// the server and exports what it logged on the way.
	defer lifecycle.HandleGracefulShutdown(ctx, log, &stack, cleanupTimeout)

	store, err := resolver.Resolve(ctx, log)
	if err != nil {
		log.Error("Could not resolve storage backend", slog.Any("error", err))
		return 1
	}

	grpcdServer := service.NewGRPCDServer(log, store, anchor)

	checks := diagnostics.Checks{service.StorageCheckName: grpcdServer.StorageCheck}

	healthSrv := health.NewServer()

	_, err = service_lib.Register(
		host.Server,
		host.HTTPHost.Mux,
		healthSrv,
		checks,
		func(rpc *connect.Server) {
			grpcdconnect.RegisterGRPCDServiceHandler(rpc, grpcdServer)
		})
	if err != nil {
		log.Error("Failed to register services", slog.Any("error", err))
		return 1
	}

	// The store's reachability is this service's health. Whatever routes to
	// this instance can ask and decide whether to keep sending clients; the
	// handlers hold what they have and wait for the store either way. The ""
	// entry stays SERVING: the process is alive.
	go grpcdServer.ReportHealth(serveCtx, healthSrv)

	// A client that cannot reach an address has it removed, and that client may
	// be wrong. This watches for removals of addresses this process anchors and
	// writes them back, because holding their registration stream is proof the
	// service is up.
	removals, err := store.Watch(serveCtx, anchor)
	if err != nil {
		log.Error("Failed to watch for removals", slog.Any("error", err))
		return 1
	}

	go func() {
		for removal := range removals {
			grpcdServer.Reinstate(serveCtx, removal)
		}
	}()

	lis, err := net.Listen("tcp", svcCfg.Address)
	if err != nil {
		log.Error("Failed to create listener", slog.Any("error", err))
		return 1
	}

	log = log.With(slog.String("address", lis.Addr().String()))

	// Serve blocks, and a deferred teardown cannot run while it does, so it goes
	// to a goroutine and the select below decides when this returns.
	serveErr := make(chan error, 1)
	go func() { serveErr <- host.Serve(lis) }()

	log.Info("Connect server listening")

	select {
	case err := <-serveErr:
		if err != nil {
			log.Error("Failed to serve", slog.Any("error", err))
			return 1
		}
	case <-serveCtx.Done():
	}

	return 0
}
