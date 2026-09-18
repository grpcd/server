package storage

import (
	"context"
	"iter"
	"sync/atomic"
)

// Condition is the store's reachability at a moment: Lost while an operation
// has failed against the backend and its subscription has not come back, and
// Changed closed when that changes.
//
// A handler that cannot proceed sleeps on Changed and looks again. Closing it
// wakes every sleeper at once, with no record of who they were.
type Condition struct {
	Lost    bool
	Changed chan struct{}
}

// Conditions holds the current Condition. One per store. It is replaced whole
// and never written to, so a reader sees one consistent pair.
type Conditions struct {
	current atomic.Pointer[Condition]
}

// NewConditions starts healthy.
func NewConditions() *Conditions {
	c := &Conditions{}
	c.current.Store(&Condition{Changed: make(chan struct{})})

	return c
}

// Current answers with the store's reachability now.
func (c *Conditions) Current() *Condition {
	return c.current.Load()
}

// Changes yields each condition as it lands, the current one first, until
// ctx ends or the consumer stops. Each is yielded after the previous one's
// Changed closed, so none is slept through.
func (c *Conditions) Changes(ctx context.Context) iter.Seq[*Condition] {
	return func(yield func(*Condition) bool) {
		for {
			condition := c.Current()

			if !yield(condition) {
				return
			}

			select {
			case <-condition.Changed:
			case <-ctx.Done():
				return
			}
		}
	}
}

// Lose records that the backend could not be reached. A store already lost
// stays as it is, so one outage is announced once however many operations
// fail during it. The swap is retried when another change lands first.
func (c *Conditions) Lose() {
	for {
		before := c.current.Load()

		if before.Lost {
			return
		}

		if c.current.CompareAndSwap(before, &Condition{Lost: true, Changed: make(chan struct{})}) {
			close(before.Changed)

			return
		}
	}
}

// Recover records that the backend's subscription came back. It announces
// whether or not a loss was seen first: an instance whose link went silent
// and returned missed whatever was published meanwhile, and everything
// sleeping on the previous condition is woken to look again.
func (c *Conditions) Recover() {
	previous := c.current.Swap(&Condition{Changed: make(chan struct{})})

	close(previous.Changed)
}
