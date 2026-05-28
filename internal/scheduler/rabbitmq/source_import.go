// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"blackcat.ca/gmeow/internal/contracts"
)

type SourceImportPublisher interface {
	PublishSourceImportJob(context.Context, contracts.SourceImportJob) error
	ProcessSourceImportFailures(context.Context, int) (int, error)
	SourceImportStatus(context.Context) (contracts.SourceImportQueueStatus, error)
}

func (broker *Broker) declareSourceImportQueues(channel *amqp.Channel) error {
	if err := channel.ExchangeDeclare(
		broker.topology.sourceImportExchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare source import exchange: %w", err)
	}

	if _, err := channel.QueueDeclare(
		broker.topology.sourceImportWorkQueue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-max-priority":            int32(100),
			"x-dead-letter-exchange":    broker.topology.sourceImportExchange,
			"x-dead-letter-routing-key": sourceImportFailedRoutingKey,
		},
	); err != nil {
		return fmt.Errorf("declare source import work queue: %w", err)
	}
	if _, err := channel.QueueDeclare(
		broker.topology.sourceImportRetryQueue,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange":    broker.topology.sourceImportExchange,
			"x-dead-letter-routing-key": sourceImportWorkRoutingKey,
		},
	); err != nil {
		return fmt.Errorf("declare source import retry queue: %w", err)
	}
	if _, err := channel.QueueDeclare(
		broker.topology.sourceImportFailedQueue,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare source import failed queue: %w", err)
	}
	if _, err := channel.QueueDeclare(
		broker.topology.sourceImportDeadLetterQueue,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare source import dead-letter queue: %w", err)
	}

	return nil
}

func (broker *Broker) sourceImportBindings() []queueBinding {
	return []queueBinding{
		{
			broker.topology.sourceImportWorkQueue,
			sourceImportWorkRoutingKey,
			broker.topology.sourceImportExchange,
		},
		{
			broker.topology.sourceImportRetryQueue,
			sourceImportRetryRoutingKey,
			broker.topology.sourceImportExchange,
		},
		{
			broker.topology.sourceImportFailedQueue,
			sourceImportFailedRoutingKey,
			broker.topology.sourceImportExchange,
		},
		{
			broker.topology.sourceImportDeadLetterQueue,
			sourceImportDeadLetterRoutingKey,
			broker.topology.sourceImportExchange,
		},
	}
}

type SourceImportJobSourceConfig struct {
	URL         string
	QueuePrefix string
	Prefetch    int
}

type SourceImportJobSource struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	deliveries <-chan amqp.Delivery
	config     SourceImportJobSourceConfig
	mutex      sync.Mutex
}

func NewSourceImportJobSource(
	ctx context.Context,
	config SourceImportJobSourceConfig,
) (*SourceImportJobSource, error) {
	if strings.TrimSpace(config.URL) == "" {
		return nil, errors.New("scheduler rabbitmq url is required")
	}
	if strings.TrimSpace(config.QueuePrefix) == "" {
		config.QueuePrefix = defaultQueuePrefix
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	connection, err := amqp.DialConfig(config.URL, amqp.Config{
		Properties: amqp.Table{"connection_name": "gmeow.scheduler.source_import"},
	})
	if err != nil {
		return nil, fmt.Errorf("connect scheduler rabbitmq source import: %w", err)
	}

	return &SourceImportJobSource{config: config, connection: connection}, nil
}

func (broker *Broker) PublishSourceImportJob(
	ctx context.Context,
	job contracts.SourceImportJob,
) error {
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
		broker.topology.sourceImportExchange,
		sourceImportWorkRoutingKey,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    job.IdempotencyKey,
			Timestamp:    time.Now().UTC(),
			Priority:     uint8(clampPriority(job.Priority)),
			Headers: amqp.Table{
				"idempotency_key": job.IdempotencyKey,
				"attempt":         int32(job.Attempt),
				"run_id":          job.RunID,
			},
			Body: body,
		},
	)
}

func (broker *Broker) SourceImportStatus(
	ctx context.Context,
) (contracts.SourceImportQueueStatus, error) {
	channel, err := broker.channel(ctx)
	if err != nil {
		return contracts.SourceImportQueueStatus{}, err
	}
	defer channel.Close()

	work, err := channel.QueueInspect(broker.topology.sourceImportWorkQueue)
	if err != nil {
		return contracts.SourceImportQueueStatus{}, err
	}
	retry, err := channel.QueueInspect(broker.topology.sourceImportRetryQueue)
	if err != nil {
		return contracts.SourceImportQueueStatus{}, err
	}
	failed, err := channel.QueueInspect(broker.topology.sourceImportFailedQueue)
	if err != nil {
		return contracts.SourceImportQueueStatus{}, err
	}
	dead, err := channel.QueueInspect(broker.topology.sourceImportDeadLetterQueue)
	if err != nil {
		return contracts.SourceImportQueueStatus{}, err
	}

	return contracts.SourceImportQueueStatus{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Pending:       work.Messages,
		Retry:         retry.Messages,
		Failed:        failed.Messages,
		DeadLetter:    dead.Messages,
	}, nil
}

func (broker *Broker) ProcessSourceImportFailures(
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
	confirms, err := enablePublishConfirms(channel)
	if err != nil {
		return 0, err
	}

	processed := 0
	for processed < limit {
		delivery, ok, err := channel.Get(
			broker.topology.sourceImportFailedQueue,
			false,
		)
		if err != nil {
			return processed, err
		}
		if !ok {
			break
		}

		var job contracts.SourceImportJob
		if err := json.Unmarshal(delivery.Body, &job); err != nil {
			_ = delivery.Nack(false, true)

			return processed, err
		}

		job.Attempt++
		routingKey := sourceImportRetryRoutingKey
		if job.Attempt > broker.config.RetryLimit {
			routingKey = sourceImportDeadLetterRoutingKey
		}

		body, err := json.Marshal(job)
		if err != nil {
			_ = delivery.Nack(false, true)

			return processed, err
		}

		if err := publishAndWaitConfirmed(
			ctx,
			channel,
			confirms,
			broker.topology.sourceImportExchange,
			routingKey,
			broker.sourceImportRetryPublishing(job, body),
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

func (broker *Broker) sourceImportRetryPublishing(
	job contracts.SourceImportJob,
	body []byte,
) amqp.Publishing {
	backoff := broker.retryBackoff(job.Attempt)
	headers := amqp.Table{
		"idempotency_key": job.IdempotencyKey,
		"attempt":         int32(job.Attempt),
		"run_id":          job.RunID,
	}
	if backoff > 0 {
		headers["retry_backoff_ms"] = int64(backoff / time.Millisecond)
	}

	publishing := amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    job.IdempotencyKey,
		Timestamp:    time.Now().UTC(),
		Priority:     uint8(clampPriority(job.Priority)),
		Headers:      headers,
		Body:         body,
	}
	if backoff > 0 && job.Attempt <= broker.config.RetryLimit {
		publishing.Expiration = fmt.Sprintf("%d", backoff/time.Millisecond)
	}

	return publishing
}

func (source *SourceImportJobSource) Receive(
	ctx context.Context,
) (SourceImportReceipt, error) {
	if err := source.ensureConsumer(ctx); err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case delivery, ok := <-source.deliveries:
		if !ok {
			return nil, errors.New("scheduler rabbitmq source import delivery channel closed")
		}

		var job contracts.SourceImportJob
		if err := json.Unmarshal(delivery.Body, &job); err != nil {
			_ = delivery.Nack(false, false)

			return nil, fmt.Errorf("decode source import job: %w", err)
		}

		return &SourceImportJobReceipt{
			source:   source,
			delivery: delivery,
			job:      job,
		}, nil
	}
}

func (source *SourceImportJobSource) Close() error {
	source.mutex.Lock()
	defer source.mutex.Unlock()

	var err error
	if source.channel != nil {
		err = source.channel.Close()
		source.channel = nil
	}
	if source.connection != nil {
		if closeErr := source.connection.Close(); err == nil {
			err = closeErr
		}
		source.connection = nil
	}

	return err
}

func (source *SourceImportJobSource) ensureConsumer(ctx context.Context) error {
	source.mutex.Lock()
	defer source.mutex.Unlock()

	if source.deliveries != nil {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	channel, err := source.connection.Channel()
	if err != nil {
		return fmt.Errorf("open scheduler rabbitmq source import channel: %w", err)
	}

	prefetch := source.config.Prefetch
	if prefetch <= 0 {
		prefetch = 1
	}
	if err := channel.Qos(prefetch, 0, false); err != nil {
		_ = channel.Close()

		return fmt.Errorf("set scheduler rabbitmq source import qos: %w", err)
	}

	deliveries, err := channel.ConsumeWithContext(
		ctx,
		source.config.QueuePrefix+sourceImportWorkSuffix,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = channel.Close()

		return fmt.Errorf("consume source import work queue: %w", err)
	}

	source.channel = channel
	source.deliveries = deliveries

	return nil
}

func (source *SourceImportJobSource) publishFailure(
	ctx context.Context,
	job contracts.SourceImportJob,
	cause error,
) error {
	if cause != nil {
		job.Failure = cause.Error()
	}
	body, err := json.Marshal(job)
	if err != nil {
		return err
	}

	channel, err := source.connection.Channel()
	if err != nil {
		return fmt.Errorf("open scheduler rabbitmq source import failure channel: %w", err)
	}
	defer channel.Close()

	if err := channel.Confirm(false); err != nil {
		return fmt.Errorf("enable scheduler rabbitmq source import failure confirms: %w", err)
	}
	confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))

	headers := amqp.Table{"attempt": int32(job.Attempt)}
	if cause != nil {
		headers["failure"] = cause.Error()
	}

	if err := channel.PublishWithContext(
		ctx,
		source.config.QueuePrefix+sourceImportExSuffix,
		sourceImportFailedRoutingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    job.IdempotencyKey,
			Timestamp:    time.Now().UTC(),
			Priority:     uint8(clampPriority(job.Priority)),
			Headers:      headers,
			Body:         body,
		},
	); err != nil {
		return err
	}

	select {
	case confirmation := <-confirms:
		if !confirmation.Ack {
			return errors.New("source import failure publish was not confirmed")
		}

		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type SourceImportJobReceipt struct {
	source   *SourceImportJobSource
	delivery amqp.Delivery
	job      contracts.SourceImportJob
}

type SourceImportReceipt interface {
	Job() contracts.SourceImportJob
	Ack(context.Context) error
	Retry(context.Context, error) error
}

func (receipt *SourceImportJobReceipt) Job() contracts.SourceImportJob {
	return receipt.job
}

func (receipt *SourceImportJobReceipt) Ack(context.Context) error {
	receipt.source.mutex.Lock()
	defer receipt.source.mutex.Unlock()

	return receipt.delivery.Ack(false)
}

func (receipt *SourceImportJobReceipt) Retry(ctx context.Context, cause error) error {
	err := receipt.source.publishFailure(ctx, receipt.job, cause)
	if err != nil {
		receipt.source.mutex.Lock()
		defer receipt.source.mutex.Unlock()
		_ = receipt.delivery.Nack(false, true)

		return err
	}

	receipt.source.mutex.Lock()
	defer receipt.source.mutex.Unlock()

	return receipt.delivery.Ack(false)
}

var _ SourceImportPublisher = (*Broker)(nil)
