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
	exchangeName           = "gmeow:analysis"
	projectionExchangeName = "gmeow:projection"
	workQueueName          = "gmeow:analysis:work"
	retryQueueName         = "gmeow:analysis:retry"
	deadLetterQueueName    = "gmeow:analysis:dead"
	projectionQueueName    = "gmeow:projection:refresh"
	workRoutingKey         = "analysis.work"
	retryRoutingKey        = "analysis.retry"
	deadLetterRoutingKey   = "analysis.dead"
	projectionRoutingKey   = "projection.refresh"
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
}

func New(ctx context.Context, cfg Config) (*Broker, error) {
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, errors.New("rabbitmq url is required")
	}
	if cfg.QueuePrefix != "" && !strings.HasPrefix(cfg.QueuePrefix, "gmeow:") {
		return nil, errors.New("rabbitmq queue prefix must start with gmeow:")
	}
	if cfg.RetryLimit <= 0 {
		cfg.RetryLimit = 3
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = 30 * time.Second
	}
	conn, err := amqp.DialConfig(cfg.URL, amqp.Config{
		Properties: amqp.Table{"connection_name": "gmeow:scheduler"},
	})
	if err != nil {
		return nil, fmt.Errorf("connect rabbitmq: %w", err)
	}
	broker := &Broker{connection: conn, config: cfg}
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

func (broker *Broker) Declare(ctx context.Context) error {
	channel, err := broker.channel(ctx)
	if err != nil {
		return err
	}
	defer channel.Close()
	if err := channel.ExchangeDeclare(
		exchangeName,
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
		projectionExchangeName,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare projection exchange: %w", err)
	}
	if _, err := channel.QueueDeclare(workQueueName, true, false, false, false, amqp.Table{
		"x-max-priority":            int32(100),
		"x-dead-letter-exchange":    exchangeName,
		"x-dead-letter-routing-key": retryRoutingKey,
	}); err != nil {
		return fmt.Errorf("declare work queue: %w", err)
	}
	if _, err := channel.QueueDeclare(
		retryQueueName,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-message-ttl":             int32(broker.config.RetryBackoff / time.Millisecond),
			"x-dead-letter-exchange":    exchangeName,
			"x-dead-letter-routing-key": workRoutingKey,
		},
	); err != nil {
		return fmt.Errorf("declare retry queue: %w", err)
	}
	if _, err := channel.QueueDeclare(
		deadLetterQueueName,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare dead-letter queue: %w", err)
	}
	if _, err := channel.QueueDeclare(
		projectionQueueName,
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
		{workQueueName, workRoutingKey, exchangeName},
		{retryQueueName, retryRoutingKey, exchangeName},
		{deadLetterQueueName, deadLetterRoutingKey, exchangeName},
		{projectionQueueName, projectionRoutingKey, projectionExchangeName},
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
	return channel.PublishWithContext(
		ctx,
		exchangeName,
		workRoutingKey,
		false,
		false,
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
	return channel.PublishWithContext(
		ctx,
		projectionExchangeName,
		projectionRoutingKey,
		false,
		false,
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
	work, err := channel.QueueInspect(workQueueName)
	if err != nil {
		return contracts.SchedulerStatus{}, err
	}
	retry, err := channel.QueueInspect(retryQueueName)
	if err != nil {
		return contracts.SchedulerStatus{}, err
	}
	dead, err := channel.QueueInspect(deadLetterQueueName)
	if err != nil {
		return contracts.SchedulerStatus{}, err
	}
	return contracts.SchedulerStatus{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Pending:       work.Messages,
		Retry:         retry.Messages,
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
	for len(jobs) < limit {
		delivery, ok, err := channel.Get(deadLetterQueueName, false)
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
		jobs = append(jobs, job)
		if err := delivery.Nack(false, true); err != nil {
			return nil, err
		}
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
		delivery, ok, err := channel.Get(deadLetterQueueName, false)
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
		if err := channel.PublishWithContext(
			ctx,
			exchangeName,
			workRoutingKey,
			false,
			false,
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
	return channel.PublishWithContext(
		ctx,
		exchangeName,
		routingKey,
		false,
		false,
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
