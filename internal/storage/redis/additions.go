package redis

import (
	"context"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// Listen subscribes this instance to every method's additions, once, and
// announces each as it arrives. Every handler waiting on a registration wakes
// from that one announcement, so the subscription count does not grow with
// the handlers waiting.
//
// The subscription is the one connection to the backend that is always open,
// so it is also how a loss is noticed without anything touching the store: a
// receive that fails is the backend gone, and the store is lost from then
// until the subscription is back. Every receive after a failure reconnects,
// resolving the address again, so a backend brought back elsewhere under the
// same name is found; the attempts are paced on the backoff schedule. The
// confirmation of the resubscription is the store coming back.
func (r *Store) Listen(ctx context.Context) error {
	subscription := r.client.PSubscribe(ctx, additionsPattern)

	// Confirmed before returning, so an addition published after this call
	// is announced.
	if err := confirmed(ctx, subscription); err != nil {
		subscription.Close()

		return err
	}

	go r.receive(ctx, subscription)

	return nil
}

// confirmed waits for the first subscription confirmation.
func confirmed(ctx context.Context, subscription *redis.PubSub) error {
	message, err := subscription.Receive(ctx)
	if err != nil {
		return err
	}

	if _, ok := message.(*redis.Subscription); !ok {
		return errors.New("subscription delivered a message before it was confirmed")
	}

	return nil
}

// receive announces what the subscription delivers until ctx ends, marking
// the store lost when a receive fails and pacing the reconnection it makes.
func (r *Store) receive(ctx context.Context, subscription *redis.PubSub) {
	defer subscription.Close()

	schedule := r.newBackOff()

	for ctx.Err() == nil {
		message, err := subscription.Receive(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			r.conditions.Lose()

			select {
			case <-time.After(schedule.NextBackOff()):
			case <-ctx.Done():
				return
			}

			continue
		}

		schedule.Reset()
		r.announce(message)
	}
}

// announce acts on one delivery: a resubscription means the store came back,
// a message means an address registered.
func (r *Store) announce(message interface{}) {
	switch m := message.(type) {
	case *redis.Subscription:
		r.conditions.Recover()
	case *redis.Message:
		r.additions.Announce(methodOf(m.Channel), m.Payload)
	}
}
