package beanstalk

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	beanstalk "github.com/beanstalkd/go-beanstalk"
	"github.com/retailcrm/mg-transport-core/v2/core/logger"
	"go.uber.org/zap"
)

// TubeStats is a snapshot of the tube job counters used for queue statistics.
type TubeStats struct {
	Ready    int64
	Delayed  int64
	Reserved int64
}

// ManagerInterface is the subset of beanstalkd operations required by a Backend. It is implemented by
// Manager and can be satisfied by test doubles.
type ManagerInterface interface {
	Put([]byte, uint32, time.Duration, time.Duration) (uint64, error)
	Reserve(time.Duration) (uint64, []byte, error)
	Delete(uint64) error
	Release(uint64, uint32, time.Duration) error
	Touch(uint64) error
	Attempts(uint64) (uint64, error)
	Stats() (TubeStats, error)
	Close() error
}

// Manager maintains two dedicated beanstalkd connections to a single tube: a producer connection for
// Put and Stats, and a consumer connection for Reserve and settlement calls. Both connections
// reconnect automatically (with the configured delay) whenever a network error is detected, so the
// Manager survives beanstalkd restarts. All methods are safe for concurrent use.
type Manager struct {
	address        string
	tubeName       string
	log            logger.Logger
	reconnectDelay time.Duration
	ctx            context.Context
	cancel         context.CancelFunc
	closed         atomic.Bool
	sendMu         sync.Mutex
	receiveMu      sync.Mutex
	tube           *beanstalk.Tube
	tubeSet        *beanstalk.TubeSet
}

// NewManager dials the beanstalkd server at address and binds one producer and one consumer
// connection to the tube. It retries dialing until the context is canceled; reconnectDelay throttles
// the retry loop (default one second). A nil log falls back to a no-op logger.
func NewManager(ctx context.Context, address, tube string, log logger.Logger, reconnectDelay time.Duration) (*Manager, error) {
	if reconnectDelay <= 0 {
		reconnectDelay = time.Second
	}
	if log == nil {
		log = logger.NewNil()
	}
	runtimeCtx, cancel := context.WithCancel(context.Background())
	manager := &Manager{address: address, tubeName: tube, log: log, reconnectDelay: reconnectDelay, ctx: runtimeCtx, cancel: cancel}
	if err := manager.connect(ctx); err != nil {
		return nil, err
	}
	return manager, nil
}

func (m *Manager) dial(ctx context.Context) (*beanstalk.Conn, error) {
	for {
		conn, err := beanstalk.Dial("tcp", m.address)
		if err == nil {
			return conn, nil
		}
		m.log.Warn("cannot connect to beanstalkd", zap.Error(err))
		timer := time.NewTimer(m.reconnectDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (m *Manager) connect(ctx context.Context) error {
	producer, err := m.dial(ctx)
	if err != nil {
		return err
	}
	consumer, err := m.dial(ctx)
	if err != nil {
		_ = producer.Close()
		return err
	}
	m.tube = beanstalk.NewTube(producer, m.tubeName)
	m.tubeSet = beanstalk.NewTubeSet(consumer, m.tubeName)
	return nil
}

func (m *Manager) Put(body []byte, priority uint32, delay, ttr time.Duration) (uint64, error) {
	m.sendMu.Lock()
	defer m.sendMu.Unlock()
	for {
		id, err := m.tube.Put(body, priority, delay, ttr)
		if !isNetworkError(err) || m.closed.Load() {
			return id, err
		}
		if err := m.reconnectProducerLocked(); err != nil {
			return 0, err
		}
	}
}
func (m *Manager) Reserve(timeout time.Duration) (uint64, []byte, error) {
	m.receiveMu.Lock()
	defer m.receiveMu.Unlock()
	for {
		id, body, err := m.tubeSet.Reserve(timeout)
		if !isNetworkError(err) || m.closed.Load() {
			return id, body, err
		}
		if err := m.reconnectConsumerLocked(); err != nil {
			return 0, nil, err
		}
	}
}
func (m *Manager) Delete(id uint64) error {
	m.receiveMu.Lock()
	defer m.receiveMu.Unlock()
	return m.retryConsumerLocked(func() error { return m.tubeSet.Conn.Delete(id) })
}
func (m *Manager) Release(id uint64, priority uint32, delay time.Duration) error {
	m.receiveMu.Lock()
	defer m.receiveMu.Unlock()
	return m.retryConsumerLocked(func() error { return m.tubeSet.Conn.Release(id, priority, delay) })
}
func (m *Manager) Touch(id uint64) error {
	m.receiveMu.Lock()
	defer m.receiveMu.Unlock()
	return m.retryConsumerLocked(func() error { return m.tubeSet.Conn.Touch(id) })
}
func (m *Manager) Attempts(id uint64) (uint64, error) {
	m.receiveMu.Lock()
	defer m.receiveMu.Unlock()
	values, err := m.tubeSet.Conn.StatsJob(id)
	if err != nil {
		return 0, err
	}
	return m.statistic[uint64](values, "reserves")
}
func (m *Manager) Stats() (TubeStats, error) {
	m.sendMu.Lock()
	defer m.sendMu.Unlock()
	values, err := m.tube.Stats()
	if err != nil {
		return TubeStats{}, err
	}
	ready, err := m.statistic[int64](values, "current-jobs-ready")
	if err != nil {
		return TubeStats{}, err
	}
	delayed, err := m.statistic[int64](values, "current-jobs-delayed")
	if err != nil {
		return TubeStats{}, err
	}
	reserved, err := m.statistic[int64](values, "current-jobs-reserved")
	if err != nil {
		return TubeStats{}, err
	}
	return TubeStats{Ready: ready, Delayed: delayed, Reserved: reserved}, nil
}
func (m *Manager) Close() error {
	m.closed.Store(true)
	m.cancel()
	m.sendMu.Lock()
	m.receiveMu.Lock()
	defer m.sendMu.Unlock()
	defer m.receiveMu.Unlock()
	return errors.Join(m.tube.Conn.Close(), m.tubeSet.Conn.Close())
}

func (m *Manager) statistic[Integer ~int64 | ~uint64](values map[string]string, key string) (Integer, error) {
	value, err := strconv.ParseUint(values[key], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse beanstalk statistic %s: %w", key, err)
	}
	return Integer(value), nil
}

func (m *Manager) retryConsumerLocked(operation func() error) error {
	for {
		err := operation()
		if !isNetworkError(err) || m.closed.Load() {
			return err
		}
		if err := m.reconnectConsumerLocked(); err != nil {
			return err
		}
	}
}

func (m *Manager) reconnectProducerLocked() error {
	_ = m.tube.Conn.Close()
	connection, err := m.dial(m.ctx)
	if err != nil {
		return err
	}
	m.tube = beanstalk.NewTube(connection, m.tubeName)
	return nil
}

func (m *Manager) reconnectConsumerLocked() error {
	_ = m.tubeSet.Conn.Close()
	connection, err := m.dial(m.ctx)
	if err != nil {
		return err
	}
	m.tubeSet = beanstalk.NewTubeSet(connection, m.tubeName)
	return nil
}

func isNetworkError(err error) bool {
	_, ok := errors.AsType[net.Error](err)
	return ok
}
