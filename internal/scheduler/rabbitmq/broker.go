// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/scheduler"
)

const (
	defaultQueuePrefix   = "gmeow."
	testQueuePrefix      = "gmeow.test."
	workSuffix           = "analysis.work"
	retrySuffix          = "analysis.retry"
	failedSuffix         = "analysis.failed"
	deadLetterSuffix     = "analysis.dead"
	projectionSuffix     = "projection.refresh"
	analysisSuffix       = "analysis"
	projectionExSuffix   = "projection"
	workRoutingKey       = "analysis.work"
	retryRoutingKey      = "analysis.retry"
	failedRoutingKey     = "analysis.failed"
	deadLetterRoutingKey = "analysis.dead"
	projectionRoutingKey = "projection.refresh"
)

type Config struct {
	URL          string
	RetryLimit   int
	RetryBackoff time.Duration
	QueuePrefix  string
}

type Broker struct {
	connection *amqp.Connection
	config     Config
	topology   topology
}

type topology struct {
	analysisExchange   string
	projectionExchange string
	workQueue          string
	retryQueue         string
	failedQueue        string
	deadLetterQueue    string
	projectionQueue    string
}

func New(ctx context.Context, cfg Config) (*Broker, error) {
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, errors.New("rabbitmq url is required")
	}
	if strings.TrimSpace(cfg.QueuePrefix) == "" {
		cfg.QueuePrefix = defaultQueuePrefix
	}
	if cfg.QueuePrefix != defaultQueuePrefix && cfg.QueuePrefix != testQueuePrefix {
		return nil, errors.New("rabbitmq queue prefix must be gmeow. or gmeow.test.")
	}
	if cfg.RetryLimit <= 0 {
		cfg.RetryLimit = 3
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = 30 * time.Second
	}
	conn, err := amqp.DialConfig(cfg.URL, amqp.Config{
		Properties: amqp.Table{"connection_name": "gmeow.scheduler"},
	})
	if err != nil {
		return nil, fmt.Errorf("connect rabbitmq: %w", err)
	}
	broker := &Broker{
		connection: conn,
		config:     cfg,
		topology:   newTopology(cfg.QueuePrefix),
	}
	if err := broker.Declare(ctx); err != nil {
		conn.Close()
		return nil, err
	}
	return broker, nil
}

func ConfigFromResolved(
	rabbit config.ResolvedRabbitMQ,
	scheduler config.ResolvedScheduler,
) Config {
	backoff, err := time.ParseDuration(scheduler.RetryBackoff)
	if err != nil {
		backoff = 30 * time.Second
	}
	return Config{
		URL:          rabbit.URL,
		RetryLimit:   scheduler.RetryLimit,
		RetryBackoff: backoff,
		QueuePrefix:  scheduler.QueuePrefix,
	}
}

func TestConfigFromResolved(
	rabbit config.ResolvedRabbitMQ,
	scheduler config.ResolvedScheduler,
) Config {
	cfg := ConfigFromResolved(rabbit, scheduler)
	cfg.URL = rabbit.TestURL
	cfg.QueuePrefix = testQueuePrefix
	return cfg
}

func newTopology(prefix string) topology {
	return topology{
		analysisExchange:   prefix + analysisSuffix,
		projectionExchange: prefix + projectionExSuffix,
		workQueue:          prefix + workSuffix,
		retryQueue:         prefix + retrySuffix,
		failedQueue:        prefix + failedSuffix,
		deadLetterQueue:    prefix + deadLetterSuffix,
		projectionQueue:    prefix + projectionSuffix,
	}
}

func (broker *Broker) Declare(ctx context.Context) error {
	channel, err := broker.channel(ctx)
	if err != nil {
		return err
	}
	defer channel.Close()
	if err := channel.ExchangeDeclare(
		broker.topology.analysisExchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare analysis exchange: %w", err)
	}
	if err := channel.ExchangeDeclare(
		broker.topology.projectionExchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare projection exchange: %w", err)
	}
	if _, err := channel.QueueDeclare(
		broker.topology.workQueue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-max-priority":            int32(100),
			"x-dead-letter-exchange":    broker.topology.analysisExchange,
			"x-dead-letter-routing-key": failedRoutingKey,
		},
	); err != nil {
		return fmt.Errorf("declare work queue: %w", err)
	}
	if _, err := channel.QueueDeclare(
		broker.topology.retryQueue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-message-ttl":             int32(broker.config.RetryBackoff / time.Millisecond),
			"x-dead-letter-exchange":    broker.topology.analysisExchange,
			"x-dead-letter-routing-key": workRoutingKey,
		},
	); err != nil {
		return fmt.Errorf("declare retry queue: %w", err)
	}
	if _, err := channel.QueueDeclare(
		broker.topology.failedQueue,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare failed queue: %w", err)
	}
	if _, err := channel.QueueDeclare(
		broker.topology.deadLetterQueue,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare dead-letter queue: %w", err)
	}
	if _, err := channel.QueueDeclare(
		broker.topology.projectionQueue,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare projection queue: %w", err)
	}
	bindings := []struct {
		queue      string
		routingKey string
		exchange   string
	}{
		{broker.topology.workQueue, workRoutingKey, broker.topology.analysisExchange},
		{broker.topology.retryQueue, retryRoutingKey, broker.topology.analysisExchange},
		{broker.topology.failedQueue, failedRoutingKey, broker.topology.analysisExchange},
		{
			broker.topology.deadLetterQueue,
			deadLetterRoutingKey,
			broker.topology.analysisExchange,
		},
		{
			broker.topology.projectionQueue,
			projectionRoutingKey,
			broker.topology.projectionExchange,
		},
	}
	for _, binding := range bindings {
		if err := channel.QueueBind(
			binding.queue,
			binding.routingKey,
			binding.exchange,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("bind queue %s: %w", binding.queue, err)
		}
	}
	return nil
}

func (broker *Broker) Publish(ctx context.Context, job contracts.AnalyzerJob) error {
	body, err := json.Marshal(job)
	if err != nil {
		return err
	}
	channel, err := broker.channel(ctx)
	if err != nil {
		return err
	}
	defer channel.Close()
	return publishConfirmed(
		ctx,
		channel,
		broker.topology.analysisExchange,
		workRoutingKey,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    job.IdempotencyKey,
			Timestamp:    time.Now().UTC(),
			Priority:     uint8(clampPriority(job.Priority)),
			Headers: amqp.Table{
				"idempotency_key": job.IdempotencyKey,
				"attempt":         int32(job.Attempt),
			},
			Body: body,
		},
	)
}

func (broker *Broker) PublishProjectionRefresh(
	ctx context.Context,
	digest contracts.ObjectDigest,
) error {
	body, err := json.Marshal(map[string]string{"digest": string(digest)})
	if err != nil {
		return err
	}
	channel, err := broker.channel(ctx)
	if err != nil {
		return err
	}
	defer channel.Close()
	return publishConfirmed(
		ctx,
		channel,
		broker.topology.projectionExchange,
		projectionRoutingKey,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    "projection:" + string(digest),
			Timestamp:    time.Now().UTC(),
			Body:         body,
		},
	)
}

func (broker *Broker) Status(ctx context.Context) (contracts.SchedulerStatus, error) {
	channel, err := broker.channel(ctx)
	if err != nil {
		return contracts.SchedulerStatus{}, err
	}
	defer channel.Close()
	work, err := channel.QueueInspect(broker.topology.workQueue)
	if err != nil {
		return contracts.SchedulerStatus{}, err
	}
	retry, err := channel.QueueInspect(broker.topology.retryQueue)
	if err != nil {
		return contracts.SchedulerStatus{}, err
	}
	failed, err := channel.QueueInspect(broker.topology.failedQueue)
	if err != nil {
		return contracts.SchedulerStatus{}, err
	}
	dead, err := channel.QueueInspect(broker.topology.deadLetterQueue)
	if err != nil {
		return contracts.SchedulerStatus{}, err
	}
	return contracts.SchedulerStatus{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Pending:       work.Messages,
		Retry:         retry.Messages,
		Failed:        failed.Messages,
		DeadLetter:    dead.Messages,
	}, nil
}

func (broker *Broker) DeadLetters(
	ctx context.Context,
	limit int,
) ([]contracts.AnalyzerJob, error) {
	if limit <= 0 {
		limit = 20
	}
	channel, err := broker.channel(ctx)
	if err != nil {
		return nil, err
	}
	defer channel.Close()
	jobs := []contracts.AnalyzerJob{}
	deliveries := []amqp.Delivery{}
	defer func() {
		for _, delivery := range deliveries {
			_ = delivery.Nack(false, true)
		}
	}()
	for len(jobs) < limit {
		delivery, ok, err := channel.Get(broker.topology.deadLetterQueue, false)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		var job contracts.AnalyzerJob
		if err := json.Unmarshal(delivery.Body, &job); err != nil {
			_ = delivery.Nack(false, true)
			return nil, err
		}
		deliveries = append(deliveries, delivery)
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func (broker *Broker) RequeueDeadLetters(
	ctx context.Context,
	limit int,
) (int, error) {
	if limit <= 0 {
		limit = 20
	}
	channel, err := broker.channel(ctx)
	if err != nil {
		return 0, err
	}
	defer channel.Close()
	requeued := 0
	for requeued < limit {
		delivery, ok, err := channel.Get(broker.topology.deadLetterQueue, false)
		if err != nil {
			return requeued, err
		}
		if !ok {
			break
		}
		var job contracts.AnalyzerJob
		if err := json.Unmarshal(delivery.Body, &job); err != nil {
			_ = delivery.Nack(false, true)
			return requeued, err
		}
		job.Attempt = 0
		body, err := json.Marshal(job)
		if err != nil {
			_ = delivery.Nack(false, true)
			return requeued, err
		}
		if err := publishConfirmed(
			ctx,
			channel,
			broker.topology.analysisExchange,
			workRoutingKey,
			amqp.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp.Persistent,
				MessageId:    job.IdempotencyKey,
				Timestamp:    time.Now().UTC(),
				Priority:     uint8(clampPriority(job.Priority)),
				Body:         body,
			},
		); err != nil {
			_ = delivery.Nack(false, true)
			return requeued, err
		}
		if err := delivery.Ack(false); err != nil {
			return requeued, err
		}
		requeued++
	}
	return requeued, nil
}

func (broker *Broker) ProcessFailures(
	ctx context.Context,
	limit int,
) (int, error) {
	if limit <= 0 {
		limit = 100
	}
	channel, err := broker.channel(ctx)
	if err != nil {
		return 0, err
	}
	defer channel.Close()
	processed := 0
	for processed < limit {
		delivery, ok, err := channel.Get(broker.topology.failedQueue, false)
		if err != nil {
			return processed, err
		}
		if !ok {
			break
		}
		var job contracts.AnalyzerJob
		if err := json.Unmarshal(delivery.Body, &job); err != nil {
			_ = delivery.Nack(false, true)
			return processed, err
		}
		job.Attempt++
		routingKey := retryRoutingKey
		if job.Attempt > broker.config.RetryLimit {
			routingKey = deadLetterRoutingKey
		}
		body, err := json.Marshal(job)
		if err != nil {
			_ = delivery.Nack(false, true)
			return processed, err
		}
		if err := publishConfirmed(
			ctx,
			channel,
			broker.topology.analysisExchange,
			routingKey,
			amqp.Publishing{
				ContentType:  "application/json",
				DeliveryMode: amqp.Persistent,
				MessageId:    job.IdempotencyKey,
				Timestamp:    time.Now().UTC(),
				Priority:     uint8(clampPriority(job.Priority)),
				Headers: amqp.Table{
					"idempotency_key": job.IdempotencyKey,
					"attempt":         int32(job.Attempt),
				},
				Body: body,
			},
		); err != nil {
			_ = delivery.Nack(false, true)
			return processed, err
		}
		if err := delivery.Ack(false); err != nil {
			return processed, err
		}
		processed++
	}
	return processed, nil
}

func (broker *Broker) RouteFailure(
	ctx context.Context,
	job contracts.AnalyzerJob,
) error {
	channel, err := broker.channel(ctx)
	if err != nil {
		return err
	}
	defer channel.Close()
	job.Attempt++
	routingKey := retryRoutingKey
	if job.Attempt > broker.config.RetryLimit {
		routingKey = deadLetterRoutingKey
	}
	body, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return publishConfirmed(
		ctx,
		channel,
		broker.topology.analysisExchange,
		routingKey,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    job.IdempotencyKey,
			Timestamp:    time.Now().UTC(),
			Priority:     uint8(clampPriority(job.Priority)),
			Body:         body,
		},
	)
}

func publishConfirmed(
	ctx context.Context,
	channel *amqp.Channel,
	exchange string,
	routingKey string,
	publishing amqp.Publishing,
) error {
	if err := channel.Confirm(false); err != nil {
		return fmt.Errorf("enable publish confirms: %w", err)
	}
	confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	if err := channel.PublishWithContext(
		ctx,
		exchange,
		routingKey,
		false,
		false,
		publishing,
	); err != nil {
		return err
	}
	select {
	case confirmation := <-confirms:
		if !confirmation.Ack {
			return errors.New("rabbitmq publish was not confirmed")
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (broker *Broker) Close() error {
	if broker.connection == nil {
		return nil
	}
	return broker.connection.Close()
}

func (broker *Broker) channel(ctx context.Context) (*amqp.Channel, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if broker.connection == nil {
		return nil, errors.New("rabbitmq broker is closed")
	}
	channel, err := broker.connection.Channel()
	if err != nil {
		return nil, fmt.Errorf("open rabbitmq channel: %w", err)
	}
	return channel, nil
}

func clampPriority(priority int) int {
	if priority < 0 {
		return 0
	}
	if priority > 100 {
		return 100
	}
	return priority
}

var _ scheduler.Broker = (*Broker)(nil)
