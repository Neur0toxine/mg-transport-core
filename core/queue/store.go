package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

const drainPollInterval = 10 * time.Millisecond

type BackendConstructor[T any] func(context.Context, int) (Backend[T], error)

type StoreOption[T any] func(*Store[T])

func WithPanicHandler[T any](handler PanicHandler[T]) StoreOption[T] {
	return func(store *Store[T]) { store.panicHandler = handler }
}

func WithUnsettledProcessor[T any](processor UnsettledProcessor[T]) StoreOption[T] {
	return func(store *Store[T]) { store.unsettled = processor }
}

func WithWorkerFactory[T any](factory WorkerFactory[T]) StoreOption[T] {
	return func(store *Store[T]) { store.workerFactory = factory }
}

type storeEntry[T any] struct {
	ready    chan struct{}
	executor *Executor[T]
	err      error
	removed  bool
}

type Store[T any] struct {
	mu                 sync.RWMutex
	executors          map[int]*storeEntry[T]
	backendConstructor BackendConstructor[T]
	processor          Processor[T]
	policy             WorkerPolicy
	panicHandler       PanicHandler[T]
	unsettled          UnsettledProcessor[T]
	workerFactory      WorkerFactory[T]
	closing            []*storeEntry[T]
	stopped            bool
	intakeClosed       bool
}

func NewStore[T any](constructor BackendConstructor[T], processor Processor[T], policy WorkerPolicy,
	options ...StoreOption[T],
) (*Store[T], error) {
	if constructor == nil {
		return nil, errors.New("backend constructor is required")
	}
	if processor == nil {
		return nil, errors.New("processor is required")
	}
	if err := policy.validate(); err != nil {
		return nil, err
	}
	store := &Store[T]{
		executors: make(map[int]*storeEntry[T]), backendConstructor: constructor,
		processor: processor, policy: policy, workerFactory: defaultWorkerFactory[T],
	}
	for _, option := range options {
		option(store)
	}
	if store.workerFactory == nil {
		return nil, errors.New("worker factory is required")
	}
	return store, nil
}

func (s *Store[T]) Get(ctx context.Context, id int) (*Executor[T], error) {
	for {
		s.mu.Lock()
		if s.stopped {
			s.mu.Unlock()
			return nil, context.Canceled
		}
		if entry := s.executors[id]; entry != nil {
			s.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-entry.ready:
				s.mu.RLock()
				defer s.mu.RUnlock()
				if entry.removed {
					return nil, context.Canceled
				}
				return entry.executor, entry.err
			}
		}
		entry := &storeEntry[T]{ready: make(chan struct{})}
		s.executors[id] = entry
		s.mu.Unlock()

		backend, err := s.backendConstructor(ctx, id)
		if err != nil {
			s.finishConstruction(id, entry, nil, err)
			return nil, err
		}
		if backend == nil {
			err = errors.New("backend constructor returned nil backend")
			s.finishConstruction(id, entry, nil, err)
			return nil, err
		}
		executor := newExecutor(id, backend, s.processor, s.policy, s.panicHandler, s.unsettled, s.workerFactory)
		if err := s.finishConstruction(id, entry, executor, nil); err != nil {
			_ = executor.shutdown(context.WithoutCancel(ctx))
			return nil, err
		}
		return executor, nil
	}
}

func (s *Store[T]) finishConstruction(id int, entry *storeEntry[T], executor *Executor[T], constructionErr error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if constructionErr != nil {
		entry.err = constructionErr
		if s.executors[id] == entry {
			delete(s.executors, id)
		}
		close(entry.ready)
		return constructionErr
	}
	if s.stopped || entry.removed || s.executors[id] != entry {
		entry.err = context.Canceled
		entry.executor = executor
		close(entry.ready)
		return context.Canceled
	}
	if s.intakeClosed {
		executor.CloseIntake()
	}
	entry.executor = executor
	close(entry.ready)
	return nil
}

func (s *Store[T]) Enqueue(ctx context.Context, id int, value T, options ...EnqueueOption) error {
	executor, err := s.Get(ctx, id)
	if err != nil {
		return err
	}
	return executor.Enqueue(ctx, value, options...)
}

func (s *Store[T]) Info(ctx context.Context, id int) (ExecutorInfo, bool, error) {
	s.mu.RLock()
	entry := s.executors[id]
	s.mu.RUnlock()
	if entry == nil {
		return ExecutorInfo{}, false, nil
	}
	select {
	case <-ctx.Done():
		return ExecutorInfo{}, false, ctx.Err()
	case <-entry.ready:
	}
	s.mu.RLock()
	executor, entryErr, removed := entry.executor, entry.err, entry.removed
	s.mu.RUnlock()
	if entryErr != nil || executor == nil || removed {
		return ExecutorInfo{}, false, entryErr
	}
	info, err := executor.Info(ctx)
	return info, true, err
}

func (s *Store[T]) Has(id int) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.executors[id] != nil
}

func (s *Store[T]) Reconcile(ctx context.Context, ids []int) error {
	desired := make(map[int]struct{}, len(ids))
	var errs []error
	for _, id := range ids {
		if _, exists := desired[id]; exists {
			continue
		}
		desired[id] = struct{}{}
		if _, err := s.Get(ctx, id); err != nil {
			errs = append(errs, fmt.Errorf("create queue %d: %w", id, err))
		}
	}
	if len(errs) > 0 {
		return errors.Join(errs...)
	}

	s.mu.RLock()
	stale := make([]int, 0, len(s.executors))
	for id := range s.executors {
		if _, keep := desired[id]; !keep {
			stale = append(stale, id)
		}
	}
	s.mu.RUnlock()
	for _, id := range stale {
		if err := s.Remove(ctx, id); err != nil {
			errs = append(errs, fmt.Errorf("remove queue %d: %w", id, err))
		}
	}
	return errors.Join(errs...)
}

func (s *Store[T]) Remove(ctx context.Context, id int) error {
	s.mu.Lock()
	entry := s.executors[id]
	if entry != nil {
		entry.removed = true
		delete(s.executors, id)
	}
	s.mu.Unlock()
	if entry == nil {
		return nil
	}
	select {
	case <-entry.ready:
	default:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-entry.ready:
		}
	}
	if entry.executor == nil {
		return entry.err
	}
	return entry.executor.Close(ctx)
}

func (s *Store[T]) CloseIntake() {
	s.mu.Lock()
	s.intakeClosed = true
	executors := s.readyExecutorsLocked()
	s.mu.Unlock()
	for _, executor := range executors {
		executor.CloseIntake()
	}
}

func (s *Store[T]) Stats(ctx context.Context) (Stats, error) {
	s.mu.RLock()
	entries := make([]*storeEntry[T], 0, len(s.executors))
	for _, entry := range s.executors {
		entries = append(entries, entry)
	}
	s.mu.RUnlock()
	var total Stats
	var errs []error
	for _, entry := range entries {
		select {
		case <-ctx.Done():
			return total, errors.Join(append(errs, ctx.Err())...)
		case <-entry.ready:
		}
		s.mu.RLock()
		executor, entryErr, removed := entry.executor, entry.err, entry.removed
		s.mu.RUnlock()
		if executor == nil || entryErr != nil || removed {
			continue
		}
		info, err := executor.Info(ctx)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		total.Ready += info.Stats.Ready
		total.Deferred += info.Stats.Deferred
		total.InFlight += info.Stats.InFlight
	}
	return total, errors.Join(errs...)
}

func (s *Store[T]) Drain(ctx context.Context) error {
	ticker := time.NewTicker(drainPollInterval)
	defer ticker.Stop()
	for {
		stats, err := s.Stats(ctx)
		if err != nil {
			return err
		}
		if stats.Queued() == 0 && stats.InFlight == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (s *Store[T]) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.stopped {
		s.stopped = true
		s.closing = make([]*storeEntry[T], 0, len(s.executors))
		for _, entry := range s.executors {
			entry.removed = true
			s.closing = append(s.closing, entry)
		}
		clear(s.executors)
	}
	entries := append([]*storeEntry[T](nil), s.closing...)
	s.mu.Unlock()

	var errs []error
	for _, entry := range entries {
		select {
		case <-entry.ready:
		default:
			select {
			case <-ctx.Done():
				return errors.Join(append(errs, ctx.Err())...)
			case <-entry.ready:
			}
		}
		if entry.executor != nil {
			errs = append(errs, entry.executor.Close(ctx))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	s.mu.Lock()
	clear(s.closing)
	s.closing = nil
	s.mu.Unlock()
	return nil
}

func (s *Store[T]) readyExecutorsLocked() []*Executor[T] {
	executors := make([]*Executor[T], 0, len(s.executors))
	for _, entry := range s.executors {
		select {
		case <-entry.ready:
			if entry.executor != nil && entry.err == nil {
				executors = append(executors, entry.executor)
			}
		default:
		}
	}
	return executors
}
