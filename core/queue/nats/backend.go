package nats

import (
	"context"
	json "encoding/json/v2"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
	"uuid"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	corenats "github.com/retailcrm/mg-transport-core/v2/core/nats"
	"github.com/retailcrm/mg-transport-core/v2/core/queue"
)

type ProvisionMode uint8

const (
	BindExisting ProvisionMode = iota
	Ensure
)

type Config struct {
	Subject         string
	ScheduleSubject string
	Stream          jetstream.StreamConfig
	Consumer        jetstream.ConsumerConfig
	Provision       ProvisionMode
	FetchMaxWait    time.Duration
}

type Backend[T any] struct {
	js       jetstream.JetStream
	stream   jetstream.Stream
	consumer jetstream.Consumer
	codec    queue.Codec[T]
	config   Config
	closed   atomic.Bool
}

type envelope struct {
	ID         string    `json:"id"`
	EnqueuedAt time.Time `json:"enqueuedAt"`
	Payload    []byte    `json:"payload"`
}

func New[T any](ctx context.Context, client *corenats.Client, codec queue.Codec[T], config Config) (*Backend[T], error) {
	if client == nil || client.JetStream == nil {
		return nil, errors.New("NATS JetStream client is required")
	}
	if codec == nil {
		return nil, errors.New("NATS queue codec is required")
	}
	if config.Subject == "" || config.Stream.Name == "" {
		return nil, errors.New("NATS queue subject and stream name are required")
	}
	if config.ScheduleSubject == "" {
		config.ScheduleSubject = config.Subject + ".schedule"
	}
	if config.Consumer.Name == "" && config.Consumer.Durable == "" {
		return nil, errors.New("NATS durable consumer name is required")
	}
	if config.Consumer.Name == "" {
		config.Consumer.Name = config.Consumer.Durable
	}
	if config.Consumer.Durable == "" {
		config.Consumer.Durable = config.Consumer.Name
	}
	if config.FetchMaxWait <= 0 {
		config.FetchMaxWait = time.Second
	}

	b := &Backend[T]{js: client.JetStream, codec: codec, config: config}
	var err error
	if config.Provision == Ensure {
		err = b.ensure(ctx)
	} else {
		err = b.bind(ctx)
	}
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (b *Backend[T]) ensure(ctx context.Context) error {
	config := b.config.Stream
	config.AllowMsgSchedules = true
	if len(config.Subjects) == 0 {
		config.Subjects = []string{b.config.Subject, b.config.ScheduleSubject + ".>"}
	}
	if err := validateSubjects(config.Subjects, b.config.Subject, b.config.ScheduleSubject); err != nil {
		return err
	}
	stream, err := b.js.CreateOrUpdateStream(ctx, config)
	if err != nil {
		return fmt.Errorf("ensure NATS stream %q: %w", config.Name, err)
	}
	consumerConfig := b.config.Consumer
	consumerConfig.AckPolicy = jetstream.AckExplicitPolicy
	consumerConfig.FilterSubject = b.config.Subject
	consumer, err := stream.CreateOrUpdateConsumer(ctx, consumerConfig)
	if err != nil {
		return fmt.Errorf("ensure NATS consumer %q: %w", consumerConfig.Name, err)
	}
	b.stream, b.consumer = stream, consumer
	return nil
}

func (b *Backend[T]) bind(ctx context.Context) error {
	stream, err := b.js.Stream(ctx, b.config.Stream.Name)
	if err != nil {
		return fmt.Errorf("bind NATS stream %q: %w", b.config.Stream.Name, err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return fmt.Errorf("inspect NATS stream %q: %w", b.config.Stream.Name, err)
	}
	if !info.Config.AllowMsgSchedules {
		return errors.New("NATS stream does not allow message schedules")
	}
	if err := validateSubjects(info.Config.Subjects, b.config.Subject, b.config.ScheduleSubject); err != nil {
		return err
	}
	consumer, err := stream.Consumer(ctx, b.config.Consumer.Name)
	if err != nil {
		return fmt.Errorf("bind NATS consumer %q: %w", b.config.Consumer.Name, err)
	}
	consumerInfo, err := consumer.Info(ctx)
	if err != nil {
		return fmt.Errorf("inspect NATS consumer %q: %w", b.config.Consumer.Name, err)
	}
	if consumerInfo.Config.AckPolicy != jetstream.AckExplicitPolicy || consumerInfo.Config.FilterSubject != b.config.Subject {
		return errors.New("NATS consumer must use explicit acknowledgments and the configured queue subject")
	}
	b.stream, b.consumer = stream, consumer
	return nil
}

func (b *Backend[T]) Enqueue(ctx context.Context, value T, options queue.EnqueueOptions) error {
	if b.closed.Load() {
		return context.Canceled
	}
	payload, err := b.codec.Encode(value)
	if err != nil {
		return fmt.Errorf("encode NATS delivery: %w", err)
	}
	now := time.Now()
	id := uuid.New().String()
	body, err := json.Marshal(envelope{ID: id, EnqueuedAt: now, Payload: payload})
	if err != nil {
		return fmt.Errorf("encode NATS envelope: %w", err)
	}
	message := &natsgo.Msg{Subject: b.config.Subject, Data: body, Header: natsgo.Header{jetstream.MsgIDHeader: []string{id}}}
	publishOptions := []jetstream.PublishOpt{jetstream.WithMsgID(id)}
	if options.NotBefore.After(now) {
		message.Subject = b.config.ScheduleSubject + "." + strings.ReplaceAll(id, "-", "")
		publishOptions = append(publishOptions, jetstream.WithScheduleAt(options.NotBefore), jetstream.WithScheduleTarget(b.config.Subject))
	}
	if _, err := b.js.PublishMsg(ctx, message, publishOptions...); err != nil {
		return fmt.Errorf("publish NATS delivery: %w", err)
	}
	return nil
}

func (b *Backend[T]) Dequeue(ctx context.Context) (queue.Delivery[T], error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if b.closed.Load() {
			return nil, context.Canceled
		}
		message, err := b.consumer.Next(jetstream.FetchMaxWait(b.config.FetchMaxWait))
		if errors.Is(err, natsgo.ErrTimeout) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("fetch NATS delivery: %w", err)
		}
		var body envelope
		if err := json.Unmarshal(message.Data(), &body); err != nil {
			_ = message.Term()
			return nil, fmt.Errorf("decode NATS envelope: %w", err)
		}
		value, err := b.codec.Decode(body.Payload)
		if err != nil {
			_ = message.Term()
			return nil, fmt.Errorf("decode NATS delivery: %w", err)
		}
		metadata, err := message.Metadata()
		if err != nil {
			_ = message.Nak()
			return nil, fmt.Errorf("read NATS delivery metadata: %w", err)
		}
		return &delivery[T]{message: message, value: value, metadata: queue.Metadata{ID: body.ID, EnqueuedAt: body.EnqueuedAt, DeliveredAt: time.Now(), Attempt: metadata.NumDelivered}}, nil
	}
}

func (b *Backend[T]) Stats(ctx context.Context) (queue.Stats, error) {
	info, err := b.consumer.Info(ctx)
	if err != nil {
		return queue.Stats{}, err
	}
	streamInfo, err := b.stream.Info(ctx, jetstream.WithSubjectFilter(b.config.ScheduleSubject+".>"))
	if err != nil {
		return queue.Stats{}, err
	}
	var deferred uint64
	for _, count := range streamInfo.State.Subjects {
		deferred += count
	}
	return queue.Stats{Ready: int64(info.NumPending), Deferred: int64(deferred), InFlight: int64(info.NumAckPending)}, nil
}

func (b *Backend[T]) Close(context.Context) error { b.closed.Store(true); return nil }

type delivery[T any] struct {
	message  jetstream.Msg
	value    T
	metadata queue.Metadata
	settled  atomic.Bool
}

func (d *delivery[T]) Value() T                 { return d.value }
func (d *delivery[T]) Metadata() queue.Metadata { return d.metadata }
func (d *delivery[T]) Settled() bool            { return d.settled.Load() }
func (d *delivery[T]) terminal(operation func() error) error {
	if !d.settled.CompareAndSwap(false, true) {
		return queue.ErrDeliverySettled
	}
	if err := operation(); err != nil {
		d.settled.Store(false)
		return err
	}
	return nil
}
func (d *delivery[T]) Ack(ctx context.Context) error {
	return d.terminal(func() error { return d.message.DoubleAck(ctx) })
}
func (d *delivery[T]) Requeue(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return d.terminal(func() error {
		if delay > 0 {
			return d.message.NakWithDelay(delay)
		}
		return d.message.Nak()
	})
}
func (d *delivery[T]) Reject(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return d.terminal(d.message.Term)
}
func (d *delivery[T]) Touch(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d.Settled() {
		return queue.ErrDeliverySettled
	}
	return d.message.InProgress()
}

var _ queue.Backend[int] = (*Backend[int])(nil)

func validateSubjects(patterns []string, subject, scheduleSubject string) error {
	if !coveredBy(patterns, subject) {
		return fmt.Errorf("NATS stream does not cover queue subject %q", subject)
	}
	if !coveredBy(patterns, scheduleSubject+".probe") {
		return fmt.Errorf("NATS stream does not cover schedule subject %q", scheduleSubject+".>")
	}
	return nil
}

func coveredBy(patterns []string, subject string) bool {
	subjectTokens := strings.Split(subject, ".")
	for _, pattern := range patterns {
		patternTokens := strings.Split(pattern, ".")
		matched := true
		for index, token := range patternTokens {
			if token == ">" {
				return true
			}
			if index >= len(subjectTokens) || token != "*" && token != subjectTokens[index] {
				matched = false
				break
			}
		}
		if matched && len(patternTokens) == len(subjectTokens) {
			return true
		}
	}
	return false
}
