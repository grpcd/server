package storage

import (
	"context"
	"slices"
	"testing"
)

func TestConditions(t *testing.T) {
	t.Run("starts healthy with an open announcement", func(t *testing.T) {
		current := NewConditions().Current()

		if current.Lost {
			t.Error("expected the store to start healthy")
		}

		if isClosed(current.Changed) {
			t.Error("expected Changed to be open before anything happened")
		}
	})

	t.Run("losing the store announces it once", func(t *testing.T) {
		c := NewConditions()
		healthy := c.Current()

		c.Lose()

		if !isClosed(healthy.Changed) {
			t.Error("expected the healthy condition to be announced over")
		}

		lost := c.Current()

		if !lost.Lost {
			t.Error("expected the store to be lost")
		}

		c.Lose()

		if got := c.Current(); got != lost {
			t.Error("expected a second loss to change nothing")
		}

		if isClosed(lost.Changed) {
			t.Error("expected the lost condition to stay open through a repeated loss")
		}
	})

	t.Run("recovering announces it and is healthy again", func(t *testing.T) {
		c := NewConditions()
		c.Lose()
		lost := c.Current()

		c.Recover()

		if !isClosed(lost.Changed) {
			t.Error("expected the lost condition to be announced over")
		}

		if c.Current().Lost {
			t.Error("expected the store to be healthy")
		}
	})

	t.Run("recovering while healthy still announces", func(t *testing.T) {
		c := NewConditions()
		healthy := c.Current()

		c.Recover()

		if !isClosed(healthy.Changed) {
			t.Error("expected a recovery with no loss seen to wake sleepers anyway")
		}

		if c.Current().Lost {
			t.Error("expected the store to be healthy")
		}
	})

	t.Run("yields the current condition and each change after it", func(t *testing.T) {
		c := NewConditions()

		// Each condition is changed once the consumer has it, so the next
		// one is what the change produced; breaking is one of the two ways
		// out.
		var seen []bool
		for condition := range c.Changes(t.Context()) {
			seen = append(seen, condition.Lost)

			if len(seen) == 3 {
				break
			}

			if condition.Lost {
				c.Recover()
			} else {
				c.Lose()
			}
		}

		if want := []bool{false, true, false}; !slices.Equal(seen, want) {
			t.Errorf("seen = %v, want %v", seen, want)
		}
	})

	t.Run("stops yielding when the context ends", func(t *testing.T) {
		c := NewConditions()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		var seen int
		for range c.Changes(ctx) {
			seen++

			// Ended while the consumer holds a condition nothing will change,
			// which is the other way out.
			cancel()
		}

		if seen != 1 {
			t.Errorf("seen = %d, want the current condition alone", seen)
		}
	})
}
