//revive:disable:package-comments
package mock

import (
	"context"
	"iter"
	"slices"
	"sync"

	"github.com/grpcd/server/internal/storage"
)

// set stores a set of strings
type set map[string]bool

// Store is an in-memory implementation of storage.Store for testing.
//
// It holds the same three facts the Redis store holds — which addresses serve a
// method, which instance anchors an address, and the removals an anchor has
// been told about — so a test exercises the service layer without a backend.
type Store struct {
	mu sync.RWMutex

	methods  map[string]set    // method → addresses
	anchors  map[string]string // address → anchor
	watchers map[string]chan storage.Removal

	// additions announces each Add, the way the Redis store's subscription
	// announces each publish.
	additions *storage.Additions

	// conditions is the store's reachability, which a test stages with Lose
	// and Recover alongside an injected error.
	conditions *storage.Conditions

	// adds counts calls to Add, so a test can see a handler write its rows
	// again.
	adds int

	// Signals a test registers to act once a handler has reached a point it
	// cannot otherwise observe: the next Add, the next injected failure, the
	// next look at the condition, the first look at it after a failure.
	added       chan struct{}
	failed      chan struct{}
	conditioned chan struct{}
	waiting     chan struct{}

	// errored is set by an injected failure and cleared by the next look at
	// the condition, which fires waiting.
	errored bool

	// exhausted is closed per method by the next draw that finds nothing, so a
	// test can act once a handler has run out and is about to wait.
	exhausted map[string]chan struct{}

	// loaded is closed by the next Latest, so a test can act once a handler
	// holds the announcement it will sleep on.
	loaded chan struct{}

	// Error injection for testing
	addErr              error
	removeErr           error
	removeFromMethodErr error
	addressesForErr     error
	countErr            error
	notifyErr           error
	watchErr            error
	pingErr             error
}

// NewStore creates a new in-memory store
func NewStore() *Store {
	return &Store{
		methods:    make(map[string]set),
		anchors:    make(map[string]string),
		watchers:   make(map[string]chan storage.Removal),
		additions:  storage.NewAdditions(),
		conditions: storage.NewConditions(),
		exhausted:  make(map[string]chan struct{}),
	}
}

// Add records that address serves each of methods, anchored to anchor, and
// announces each addition.
func (s *Store) Add(_ context.Context, address, anchor string, methods []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.adds++
	fire(&s.added)

	if s.addErr != nil {
		s.fail()

		return s.addErr
	}

	for _, method := range methods {
		if s.methods[method] == nil {
			s.methods[method] = make(set)
		}

		s.methods[method][address] = true
	}

	s.anchors[address] = anchor

	// Announced after the write lands, so a waiter woken by this finds the
	// address in the set when it looks.
	for _, method := range methods {
		s.additions.Announce(method, address)
	}

	return nil
}

// Remove takes address out of each of methods and forgets its anchor.
func (s *Store) Remove(_ context.Context, address string, methods []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.removeErr != nil {
		return s.removeErr
	}

	for _, method := range methods {
		delete(s.methods[method], address)

		if len(s.methods[method]) == 0 {
			delete(s.methods, method)
		}
	}

	delete(s.anchors, address)

	return nil
}

// RemoveFromMethod takes address out of one method's set, answering with the
// anchor it was registered under.
func (s *Store) RemoveFromMethod(_ context.Context, method, address string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.removeFromMethodErr != nil {
		return "", s.removeFromMethodErr
	}

	anchor := s.anchors[address]

	delete(s.methods[method], address)

	if len(s.methods[method]) == 0 {
		delete(s.methods, method)
	}

	return anchor, nil
}

// AddressesFor draws addresses serving method. Each pull takes the smallest
// member of the set as it is at that moment, which stands in for the Redis
// store's random draw while keeping the order a test asserts on stable. The
// sequence ends when the set is empty, so a member the caller does not remove
// is drawn again.
func (s *Store) AddressesFor(_ context.Context, method string) iter.Seq2[string, error] {
	return func(yield func(string, error) bool) {
		for {
			address, found, err := s.draw(method)

			if err != nil {
				yield("", err)

				return
			}

			if !found {
				return
			}

			if !yield(address, nil) {
				return
			}
		}
	}
}

// draw answers with the smallest address serving method, or found false when
// none does, telling anything waiting on Exhausted.
func (s *Store) draw(method string) (address string, found bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.addressesForErr != nil {
		s.fail()

		return "", false, s.addressesForErr
	}

	addresses := slices.Sorted(maps(s.methods[method]))
	if len(addresses) == 0 {
		if signal, waited := s.exhausted[method]; waited {
			close(signal)
			delete(s.exhausted, method)
		}

		return "", false, nil
	}

	return addresses[0], true, nil
}

// Count answers with how many addresses serve method.
func (s *Store) Count(_ context.Context, method string) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.countErr != nil {
		s.fail()

		return 0, s.countErr
	}

	return int64(len(s.methods[method])), nil
}

// Notify delivers removal to whoever is watching anchor.
func (s *Store) Notify(_ context.Context, anchor string, removal storage.Removal) error {
	s.mu.RLock()
	watcher, watching := s.watchers[anchor]
	err := s.notifyErr
	s.mu.RUnlock()

	if err != nil {
		return err
	}

	if !watching {
		return nil
	}

	watcher <- removal

	return nil
}

// Watch delivers the rows removed from under anchor.
func (s *Store) Watch(ctx context.Context, anchor string) (<-chan storage.Removal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.watchErr != nil {
		return nil, s.watchErr
	}

	watcher := make(chan storage.Removal)
	s.watchers[anchor] = watcher

	go func() {
		<-ctx.Done()

		s.mu.Lock()
		defer s.mu.Unlock()

		delete(s.watchers, anchor)
	}()

	return watcher, nil
}

// Latest answers with the most recent addition announced, telling anything
// waiting on Loaded.
func (s *Store) Latest() *storage.Addition {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.loaded != nil {
		close(s.loaded)
		s.loaded = nil
	}

	return s.additions.Latest()
}

// Condition answers with the store's reachability, telling anything waiting
// on Conditioned, and on Waiting when this is the first look since an
// injected failure.
func (s *Store) Condition() *storage.Condition {
	s.mu.Lock()
	fire(&s.conditioned)

	if s.errored {
		s.errored = false
		fire(&s.waiting)
	}
	s.mu.Unlock()

	return s.conditions.Current()
}

// fail tells anything waiting on Failed, and arms Waiting for the next look
// at the condition. Called under mu.
func (s *Store) fail() {
	fire(&s.failed)
	s.errored = true
}

// Changes yields the store's reachability as it changes, telling anything
// waiting on Conditioned once the consumer has acted on each.
func (s *Store) Changes(ctx context.Context) iter.Seq[*storage.Condition] {
	return func(yield func(*storage.Condition) bool) {
		for condition := range s.conditions.Changes(ctx) {
			if !yield(condition) {
				return
			}

			s.mu.Lock()
			fire(&s.conditioned)
			s.mu.Unlock()
		}
	}
}

// Adds answers with how many times Add was called, for assertions.
func (s *Store) Adds() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.adds
}

// Added answers with a channel closed by the next call to Add.
func (s *Store) Added() <-chan struct{} {
	return s.register(&s.added)
}

// Failed answers with a channel closed by the next injected error returned.
// A test registers it before staging the failure, then acts once the handler
// has seen the error and is about to wait.
func (s *Store) Failed() <-chan struct{} {
	return s.register(&s.failed)
}

// Conditioned answers with a channel closed by the next call to Condition, or
// by a Changes consumer finishing with the next condition yielded.
func (s *Store) Conditioned() <-chan struct{} {
	return s.register(&s.conditioned)
}

// Waiting answers with a channel closed by the first call to Condition after
// the next injected failure: a handler judging that failure against the
// store's condition, about to wait if the store is lost. A test registers it
// before the handler can fail, then stages the recovery once it fires.
func (s *Store) Waiting() <-chan struct{} {
	return s.register(&s.waiting)
}

// register hands out a fresh signal for slot.
func (s *Store) register(slot *chan struct{}) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	signal := make(chan struct{})
	*slot = signal

	return signal
}

// fire closes the signal in slot, if one is registered. Called under mu.
func fire(slot *chan struct{}) {
	if *slot != nil {
		close(*slot)
		*slot = nil
	}
}

// Loaded answers with a channel closed by the next call to Latest. A test
// registers it before starting a handler that sleeps on the announcement it
// loads, so an addition made once it fires reaches that handler as a wake
// rather than being announced before it looked.
func (s *Store) Loaded() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	signal := make(chan struct{})
	s.loaded = signal

	return signal
}

// Exhausted answers with a channel closed by the next draw for method that
// finds nothing. A test registers it before starting the handler, so the draw
// cannot come first, and then adds an address once it fires: the handler is
// out of candidates and waiting, so the addition reaches it as a wake rather
// than as a candidate.
func (s *Store) Exhausted(method string) <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	signal := make(chan struct{})
	s.exhausted[method] = signal

	return signal
}

// Addresses answers with the addresses serving method, for assertions.
func (s *Store) Addresses(method string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return slices.Sorted(maps(s.methods[method]))
}

// Anchor answers with the anchor recorded for address, for assertions.
func (s *Store) Anchor(address string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.anchors[address]
}

// Name of the store
func (*Store) Name() string {
	return "mock"
}

// Address of the store
func (*Store) Address() string {
	return ""
}

// maps yields a set's members, so slices.Sorted can order them.
func maps(members set) iter.Seq[string] {
	return func(yield func(string) bool) {
		for member := range members {
			if !yield(member) {
				return
			}
		}
	}
}
