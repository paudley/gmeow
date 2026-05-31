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
	RunSourceImportFailureQueues(ctx context.Context) error
	ProcessSourceImportFailures(context.Context, int) (int, error)
	SourceImportStatus(context.Context) (contracts.SourceImportQueueStatus, error)
}

const sourceImportFailureQueueLimit = 100

const sourceImportFailureQueueRunnerCount = 2

var errSourceImportDeadLetter = errors.New(
	"source import dead-letter queue received job",
)

func (broker *Broker) declareSourceImportQueues(channel *amqp.Channel) error {
	err := channel.ExchangeDeclare(
		broker.topology.sourceImportExchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
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
	connection      *amqp.Connection
	channel         *amqp.Channel
	deliveries      <-chan amqp.Delivery
	publishChannel  *amqp.Channel
	publishConfirms <-chan amqp.Confirmation
	publishReturns  <-chan amqp.Return
	config          SourceImportJobSourceConfig
	mutex           sync.Mutex
	publishMutex    sync.Mutex
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

	return broker.publishPersistent(
		ctx,
		broker.topology.sourceImportExchange,
		sourceImportWorkRoutingKey,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    job.IdempotencyKey,
			Timestamp:    time.Now().UTC(),
			Priority:     clampPriority(job.Priority),
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
	var work, retry, failed, dead amqp.Queue

	err := broker.withAdmin(ctx, func(channel *amqp.Channel) error {
		var inspectErr error

		work, inspectErr = inspectQueue(channel, broker.topology.sourceImportWorkQueue)
		if inspectErr != nil {
			return inspectErr
		}

		retry, inspectErr = inspectQueue(channel, broker.topology.sourceImportRetryQueue)
		if inspectErr != nil {
			return inspectErr
		}

		failed, inspectErr = inspectQueue(channel, broker.topology.sourceImportFailedQueue)
		if inspectErr != nil {
			return inspectErr
		}

		dead, inspectErr = inspectQueue(channel, broker.topology.sourceImportDeadLetterQueue)
		if inspectErr != nil {
			return inspectErr
		}

		return nil
	})
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

func (broker *Broker) RunSourceImportFailureQueues(ctx context.Context) error {
	errc := make(chan error, sourceImportFailureQueueRunnerCount)

	go func() {
		errc <- broker.runSourceImportFailedQueue(ctx)
	}()
	go func() {
		errc <- broker.watchSourceImportDeadLetterQueue(ctx)
	}()

	select {
	case <-ctx.Done():
		return fmt.Errorf("run source import failure queues: %w", ctx.Err())
	case err := <-errc:
		if err != nil {
			return err
		}

		return nil
	}
}

func (broker *Broker) ProcessSourceImportFailures(
	ctx context.Context,
	limit int,
) (int, error) {
	if limit <= 0 {
		limit = 100
	}

	processed := 0

	consumeErr := broker.withConsumer(ctx, func(channel *amqp.Channel) error {
		for processed < limit {
			delivery, received, getErr := broker.receiveDelivery(
				ctx,
				broker.topology.sourceImportFailedQueue,
				processed == 0,
			)
			if getErr != nil {
				return getErr
			}

			if !received {
				break
			}

			var job contracts.SourceImportJob

			unmarshalErr := json.Unmarshal(delivery.Body, &job)
			if unmarshalErr != nil {
				return nackAfterError(
					delivery,
					fmt.Errorf("decode failed source import job: %w", unmarshalErr),
				)
			}

			job.Attempt++

			routingKey := sourceImportRetryRoutingKey
			if job.Attempt > broker.config.RetryLimit {
				routingKey = sourceImportDeadLetterRoutingKey
			}

			body, marshalErr := json.Marshal(job)
			if marshalErr != nil {
				return nackAfterError(
					delivery,
					fmt.Errorf("encode retried source import job: %w", marshalErr),
				)
			}

			publishErr := broker.publishPersistent(
				ctx,
				broker.topology.sourceImportExchange,
				routingKey,
				broker.sourceImportRetryPublishing(job, body),
			)
			if publishErr != nil {
				return nackAfterError(delivery, publishErr)
			}

			ackErr := ackDelivery(delivery)
			if ackErr != nil {
				return ackErr
			}

			processed++
		}

		return nil
	})
	if consumeErr != nil {
		return processed, consumeErr
	}

	return processed, nil
}

func (broker *Broker) runSourceImportFailedQueue(ctx context.Context) error {
	for {
		_, err := broker.ProcessSourceImportFailures(ctx, sourceImportFailureQueueLimit)
		if err != nil {
			return fmt.Errorf("process source import failed queue: %w", err)
		}
	}
}

func (broker *Broker) watchSourceImportDeadLetterQueue(ctx context.Context) error {
	for {
		delivery, received, err := broker.receiveDelivery(
			ctx,
			broker.topology.sourceImportDeadLetterQueue,
			true,
		)
		if err != nil {
			return fmt.Errorf("receive source import dead-letter queue: %w", err)
		}

		if !received {
			continue
		}

		var job contracts.SourceImportJob

		unmarshalErr := json.Unmarshal(delivery.Body, &job)
		if unmarshalErr != nil {
			return nackAfterError(
				delivery,
				fmt.Errorf("decode source import dead-letter job: %w", unmarshalErr),
			)
		}

		nackErr := nackDelivery(delivery, true)
		if nackErr != nil {
			return nackErr
		}

		return fmt.Errorf("%w: %s", errSourceImportDeadLetter, job.IdempotencyKey)
	}
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
		Priority:     clampPriority(job.Priority),
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
	err := source.ensureConsumer(ctx)
	if err != nil {
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

		err := json.Unmarshal(delivery.Body, &job)
		if err != nil {
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
	source.publishMutex.Lock()

	var err error
	if source.publishChannel != nil {
		err = source.publishChannel.Close()
		source.publishChannel = nil
		source.publishConfirms = nil
		source.publishReturns = nil
	}
	source.publishMutex.Unlock()

	source.mutex.Lock()

	if source.channel != nil {
		if closeErr := source.channel.Close(); err == nil {
			err = closeErr
		}

		source.channel = nil
	}

	if source.connection != nil {
		if closeErr := source.connection.Close(); err == nil {
			err = closeErr
		}

		source.connection = nil
	}
	source.mutex.Unlock()

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

	if source.connection == nil {
		return errBrokerClosed
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

	deliveries, err := channel.Consume(
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

	headers := amqp.Table{"attempt": int32(job.Attempt)}
	if cause != nil {
		headers["failure"] = cause.Error()
	}

	return source.publishFailurePersistent(
		ctx,
		source.config.QueuePrefix+sourceImportExSuffix,
		sourceImportFailedRoutingKey,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    job.IdempotencyKey,
			Timestamp:    time.Now().UTC(),
			Priority:     clampPriority(job.Priority),
			Headers:      headers,
			Body:         body,
		},
	)
}

func (source *SourceImportJobSource) publishFailurePersistent(
	ctx context.Context,
	exchange string,
	routingKey string,
	publishing amqp.Publishing,
) error {
	source.publishMutex.Lock()
	defer source.publishMutex.Unlock()

	channel, err := source.publisher(ctx)
	if err != nil {
		return err
	}

	err = channel.PublishWithContext(ctx, exchange, routingKey, true, false, publishing)
	if err != nil {
		source.resetPublisher()

		return fmt.Errorf("publish scheduler rabbitmq source import failure: %w", err)
	}

	err = waitRabbitMQPublishConfirmed(
		ctx,
		exchange,
		routingKey,
		source.publishConfirms,
		source.publishReturns,
	)
	if err != nil {
		source.resetPublisher()

		return err
	}

	return nil
}

func (source *SourceImportJobSource) publisher(
	ctx context.Context,
) (*amqp.Channel, error) {
	if source.publishChannel != nil {
		return source.publishChannel, nil
	}

	err := ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("publish source import failure: %w", err)
	}

	source.mutex.Lock()
	if source.connection == nil {
		source.mutex.Unlock()

		return nil, errBrokerClosed
	}

	channel, err := source.connection.Channel()
	source.mutex.Unlock()

	if err != nil {
		return nil, fmt.Errorf(
			"open scheduler rabbitmq source import failure channel: %w",
			err,
		)
	}

	confirms, returns, err := configureRabbitMQPublisher(channel)
	if err != nil {
		_ = channel.Close()

		return nil, err
	}

	source.publishChannel = channel
	source.publishConfirms = confirms
	source.publishReturns = returns

	return channel, nil
}

func (source *SourceImportJobSource) resetPublisher() {
	if source.publishChannel == nil {
		return
	}

	channel := source.publishChannel

	source.publishChannel = nil
	source.publishConfirms = nil
	source.publishReturns = nil

	closeRabbitMQChannelAsync(channel)
}

type SourceImportJobReceipt struct {
	source   *SourceImportJobSource
	delivery amqp.Delivery
	job      contracts.SourceImportJob
}

type SourceImportReceipt interface {
	Job() contracts.SourceImportJob
	Ack(context.Context) error
	Release(context.Context) error
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

func (receipt *SourceImportJobReceipt) Release(context.Context) error {
	receipt.source.mutex.Lock()
	defer receipt.source.mutex.Unlock()

	return receipt.delivery.Nack(false, true)
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
