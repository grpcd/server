package redis

import (
	"context"
	"errors"
	"iter"
	"strings"

	"github.com/redis/go-redis/v9"

	"github.com/grpcd/server/internal/storage"
)

// Add records that address serves each of methods, anchored to anchor.
//
// SADD leaves a member that is already present alone, so an instance rewriting
// a row it still holds performs the same write as the one that created it.
func (r *Store) Add(ctx context.Context, address, anchor string, methods []string) error {
	pipe := r.client.TxPipeline()

	for _, method := range methods {
		pipe.SAdd(ctx, methodKey(method), address)
	}

	pipe.Set(ctx, anchorKey(address), anchor, 0)

	if _, err := pipe.Exec(ctx); err != nil {
		return r.observe(ctx, err)
	}

	// Announced after the write lands, so a waiting Discover that is woken by
	// this finds the address in the set when it looks.
	pipe = r.client.Pipeline()

	for _, method := range methods {
		pipe.Publish(ctx, additions(method), address)
	}

	_, err := pipe.Exec(ctx)

	return r.observe(ctx, err)
}

// Remove takes address out of each of methods and forgets its anchor.
func (r *Store) Remove(ctx context.Context, address string, methods []string) error {
	pipe := r.client.TxPipeline()

	for _, method := range methods {
		pipe.SRem(ctx, methodKey(method), address)
	}

	pipe.Del(ctx, anchorKey(address))

	_, err := pipe.Exec(ctx)

	return r.observe(ctx, err)
}

// RemoveFromMethod takes address out of one method's set, answering with the
// anchor it was registered under.
//
// The anchor is read before the removal so the caller has somewhere to send the
// notification even though the removal is what makes it worth sending.
func (r *Store) RemoveFromMethod(
	ctx context.Context, method, address string,
) (string, error) {
	pipe := r.client.TxPipeline()

	anchor := pipe.Get(ctx, anchorKey(address))
	pipe.SRem(ctx, methodKey(method), address)

	// A missing anchor key makes Exec report redis.Nil, which reports on the
	// Get rather than on the removal. The removal is what matters here.
	if _, err := pipe.Exec(ctx); err != nil && !isNil(err) {
		return "", r.observe(ctx, err)
	}

	value, err := anchor.Result()
	if isNil(err) {
		return "", nil
	}

	return value, r.observe(ctx, err)
}

// AddressesFor draws addresses serving method.
//
// SRANDMEMBER is O(1) and uniform over the set, and a walk is not: SSCAN from
// cursor 0 answers in the same order every call, which would offer every
// caller the same first address. Each pull is a fresh draw over what is in the
// set at that moment, so a member removed since the last pull is not drawn
// again.
func (r *Store) AddressesFor(ctx context.Context, method string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		for {
			address, err := r.client.SRandMember(ctx, methodKey(method)).Result()

			// Nil is Redis answering that the set is empty, which is where the
			// sequence ends.
			if isNil(err) {
				return
			}

			if err != nil {
				yield("", r.observe(ctx, err))

				return
			}

			if !yield(address, nil) {
				return
			}
		}
	}
}

// Count answers with how many addresses serve method. SCARD is O(1).
func (r *Store) Count(ctx context.Context, method string) (int64, error) {
	count, err := r.client.SCard(ctx, methodKey(method)).Result()

	return count, r.observe(ctx, err)
}

// Notify tells the instance identified by anchor which row was removed.
//
// Method names are rejected if they carry whitespace, so a space separates the
// two halves unambiguously.
func (r *Store) Notify(ctx context.Context, anchor string, removal storage.Removal) error {
	payload := removal.Method + " " + removal.Address

	return r.observe(ctx, r.client.Publish(ctx, channel(anchor), payload).Err())
}

// Watch delivers the addresses this instance anchored that something else
// removed.
//
// Subscribe registers the channel before returning, so a removal published
// after this call reaches the caller.
func (r *Store) Watch(ctx context.Context, anchor string) (<-chan storage.Removal, error) {
	subscription := r.client.Subscribe(ctx, channel(anchor))

	if _, err := subscription.Receive(ctx); err != nil {
		return nil, r.observe(ctx, err)
	}

	delivered := make(chan storage.Removal)

	go func() {
		defer close(delivered)
		defer subscription.Close()

		for message := range subscription.Channel() {
			method, address, found := strings.Cut(message.Payload, " ")
			if !found {
				continue
			}

			select {
			case delivered <- storage.Removal{Method: method, Address: address}:
			case <-ctx.Done():
				return
			}
		}
	}()

	return delivered, nil
}

// Ping checks if Redis is reachable
func (r *Store) Ping(ctx context.Context) error {
	return r.observe(ctx, r.client.Ping(ctx).Err())
}

// observe passes err through, recording the store as lost when it says Redis
// could not be reached, and as back when an operation succeeds against a
// store recorded lost. A missing key is an answer, and the caller's own
// context ending says nothing about Redis, so neither counts either way.
//
// The recovery matters for a Redis that stopped answering without closing
// its connections and then resumed: the subscription never reconnected, so
// nothing else says it is back.
func (r *Store) observe(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return err
	}

	if err != nil && !isNil(err) {
		r.conditions.Lose()

		return err
	}

	if r.conditions.Current().Lost {
		r.conditions.Recover()
	}

	return err
}

// isNil reports whether err is Redis answering that a key does not exist, which
// every caller here treats as an absent value rather than a failure.
func isNil(err error) bool {
	return errors.Is(err, redis.Nil)
}
