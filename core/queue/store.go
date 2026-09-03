package queue

import (
	"context"
	"errors"
	"sync"
	"time"
)

const drainPollInterval = 10 * time.Millisecond

type BackendConstructor[T any] func(context.Context, int) (Backend[T], error)

type Info struct {
	ID              int
	LastEnqueueTime time.Time
	Stats           Stats
}

type ScaleFunc func(Info, func(), func(), func() (slotsLeft, slotsActive int))

type queueState[T any] struct {
	queue         *Queue[T]
	workerCancels []context.CancelFunc
	scaleCancel   context.CancelFunc
}

type Store[T any] struct {
	mu                 sync.Mutex
	queues             map[int]*queueState[T]
	backendConstructor BackendConstructor[T]
	workerConstructor  WorkerConstructor[T]
	numWorkers         int
	maxNumWorkers      int
	scaleFunc          ScaleFunc
	scaleInterval      time.Duration
	intakeClosed       bool
}

func NewStore[T any](constructor BackendConstructor[T]) *Store[T] {
	return &Store[T]{backendConstructor: constructor, queues: make(map[int]*queueState[T]), numWorkers: 1, maxNumWorkers: 1}
}

func (s *Store[T]) WithWorkerConstructor(constructor WorkerConstructor[T]) *Store[T] {
	s.workerConstructor = constructor
	return s
}

func (s *Store[T]) WithNumWorkers(count int) *Store[T] {
	s.numWorkers = max(1, count)
	s.maxNumWorkers = max(s.maxNumWorkers, s.numWorkers)
	return s
}

func (s *Store[T]) WithMaxNumWorkers(count int) *Store[T] {
	s.maxNumWorkers = max(s.numWorkers, count)
	return s
}

func (s *Store[T]) WithScaleFunc(function ScaleFunc, interval time.Duration) *Store[T] {
	s.mu.Lock()
	s.scaleFunc, s.scaleInterval = function, interval
	states := make([]*queueState[T], 0, len(s.queues))
	for _, state := range s.queues {
		states = append(states, state)
	}
	s.mu.Unlock()
	for _, state := range states {
		go s.startScaler(state)
	}
	return s
}

func (s *Store[T]) Get(ctx context.Context, id int) (*Queue[T], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if state := s.queues[id]; state != nil {
		return state.queue, nil
	}
	backend, err := s.backendConstructor(ctx, id)
	if err != nil {
		return nil, err
	}
	q := New(id, backend)
	if s.intakeClosed {
		q.CloseIntake()
	}
	state := &queueState[T]{queue: q}
	s.queues[id] = state
	s.startWorkersLocked(state, id, s.numWorkers)
	go s.startScaler(state)
	return q, nil
}

func (s *Store[T]) startWorkersLocked(state *queueState[T], id, count int) {
	if s.workerConstructor == nil {
		return
	}
	for range count {
		ctx, cancel := context.WithCancel(state.queue.Context())
		state.workerCancels = append(state.workerCancels, cancel)
		go s.workerConstructor(ctx, id)(state.queue)
	}
}

func (s *Store[T]) startScaler(state *queueState[T]) {
	s.mu.Lock()
	if state.scaleCancel != nil {
		state.scaleCancel()
	}
	function, interval := s.scaleFunc, s.scaleInterval
	if function == nil || interval <= 0 {
		s.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(state.queue.Context())
	state.scaleCancel = cancel
	s.mu.Unlock()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats, err := state.queue.Stats(ctx)
			if err != nil {
				continue
			}
			invokeScaleFunc(function, Info{ID: state.queue.ID(), LastEnqueueTime: state.queue.LastEnqueueTime(), Stats: stats},
				func() { s.addWorker(state.queue.ID()) }, func() { s.stopWorker(state.queue.ID()) },
				func() (int, int) { return s.scalingInfo(state.queue.ID()) })
		}
	}
}

func invokeScaleFunc(function ScaleFunc, info Info, addWorker, stopWorker func(), scalingInfo func() (int, int)) {
	defer func() { _ = recover() }()
	function(info, addWorker, stopWorker, scalingInfo)
}

func (s *Store[T]) addWorker(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.queues[id]
	if state == nil || len(state.workerCancels) >= s.maxNumWorkers {
		return
	}
	s.startWorkersLocked(state, id, 1)
}

func (s *Store[T]) stopWorker(id int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.queues[id]
	if state == nil || len(state.workerCancels) <= 1 {
		return
	}
	last := len(state.workerCancels) - 1
	state.workerCancels[last]()
	state.workerCancels = state.workerCancels[:last]
}

func (s *Store[T]) scalingInfo(id int) (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.queues[id]
	if state == nil {
		return s.maxNumWorkers, 0
	}
	active := len(state.workerCancels)
	return max(0, s.maxNumWorkers-active), active
}

func (s *Store[T]) Remove(ctx context.Context, id int) error {
	s.mu.Lock()
	state := s.queues[id]
	delete(s.queues, id)
	s.mu.Unlock()
	if state == nil {
		return nil
	}
	if state.scaleCancel != nil {
		state.scaleCancel()
	}
	for _, cancel := range state.workerCancels {
		cancel()
	}
	return state.queue.Close(ctx)
}

func (s *Store[T]) CloseIntake() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.intakeClosed = true
	for _, state := range s.queues {
		state.queue.CloseIntake()
	}
}

func (s *Store[T]) Stats(ctx context.Context) (Stats, error) {
	s.mu.Lock()
	queues := make([]*Queue[T], 0, len(s.queues))
	for _, state := range s.queues {
		queues = append(queues, state.queue)
	}
	s.mu.Unlock()
	var result Stats
	var errs []error
	for _, q := range queues {
		stats, err := q.Stats(ctx)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		result.Ready += stats.Ready
		result.Deferred += stats.Deferred
		result.InFlight += stats.InFlight
	}
	return result, errors.Join(errs...)
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
	ids := make([]int, 0, len(s.queues))
	for id := range s.queues {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	var errs []error
	for _, id := range ids {
		errs = append(errs, s.Remove(ctx, id))
	}
	return errors.Join(errs...)
}
