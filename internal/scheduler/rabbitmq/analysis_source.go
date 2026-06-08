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

	"blackcat.ca/gmeow/internal/analysis"
	"blackcat.ca/gmeow/internal/contracts"
)

type AnalysisJobSourceConfig struct {
	URL         string
	QueuePrefix string
	// Analyzer names the analyzer whose per-analyzer work queue this source
	// consumes. Empty consumes the unsuffixed work queue (legacy/aggregate).
	Analyzer string
	Prefetch int
}

// AnalysisWorkQueue returns the work queue name for one analyzer under a prefix.
func AnalysisWorkQueue(prefix, analyzer string) string {
	if strings.TrimSpace(prefix) == "" {
		prefix = defaultQueuePrefix
	}

	if strings.TrimSpace(analyzer) == "" {
		return prefix + workSuffix
	}

	return prefix + workSuffix + "." + analyzer
}

type AnalysisJobSource struct {
	connection      *amqp.Connection
	channel         *amqp.Channel
	deliveries      <-chan amqp.Delivery
	publishChannel  *amqp.Channel
	publishConfirms <-chan amqp.Confirmation
	publishReturns  <-chan amqp.Return
	config          AnalysisJobSourceConfig
	mutex           sync.Mutex
	publishMutex    sync.Mutex
}

func NewAnalysisJobSource(
	ctx context.Context,
	config AnalysisJobSourceConfig,
) (*AnalysisJobSource, error) {
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
		Properties: amqp.Table{"connection_name": "gmeow.scheduler.analysis_source"},
	})
	if err != nil {
		return nil, fmt.Errorf("connect scheduler rabbitmq analysis source: %w", err)
	}

	return &AnalysisJobSource{config: config, connection: connection}, nil
}

func (source *AnalysisJobSource) Receive(
	ctx context.Context,
) (analysis.JobReceipt, error) {
	err := source.ensureConsumer(ctx)
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case delivery, ok := <-source.deliveries:
		if !ok {
			return nil, errors.New("scheduler rabbitmq analysis delivery channel closed")
		}

		var job contracts.AnalyzerJob

		err := json.Unmarshal(delivery.Body, &job)
		if err != nil {
			_ = delivery.Nack(false, false)

			return nil, fmt.Errorf("decode analysis job: %w", err)
		}

		return &analysisJobReceipt{source: source, delivery: delivery, job: job}, nil
	}
}

func (source *AnalysisJobSource) Close() error {
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
	defer source.mutex.Unlock()

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

	return err
}

func (source *AnalysisJobSource) ensureConsumer(ctx context.Context) error {
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
		return fmt.Errorf("open scheduler rabbitmq analysis channel: %w", err)
	}

	prefetch := source.config.Prefetch
	if prefetch <= 0 {
		prefetch = 1
	}

	err = channel.Qos(prefetch, 0, false)
	if err != nil {
		_ = channel.Close()

		return fmt.Errorf("set scheduler rabbitmq analysis qos: %w", err)
	}

	deliveries, err := channel.Consume(
		AnalysisWorkQueue(source.config.QueuePrefix, source.config.Analyzer),
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = channel.Close()

		return fmt.Errorf("consume analysis work queue: %w", err)
	}

	source.channel = channel
	source.deliveries = deliveries

	return nil
}

func (source *AnalysisJobSource) publishFailure(
	ctx context.Context,
	job contracts.AnalyzerJob,
	cause error,
) error {
	if cause != nil {
		job.Failure = cause.Error()
	}

	body, err := json.Marshal(job)
	if err != nil {
		return err
	}

	headers := amqp.Table{"attempt": contracts.ClampInt32(job.Attempt)}
	if cause != nil {
		headers["failure"] = cause.Error()
	}

	return source.publishFailurePersistent(
		ctx,
		source.config.QueuePrefix+analysisSuffix,
		failedRoutingKey,
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

func (source *AnalysisJobSource) publishFailurePersistent(
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

		return fmt.Errorf("publish scheduler rabbitmq analysis failure: %w", err)
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

func (source *AnalysisJobSource) publisher(
	ctx context.Context,
) (*amqp.Channel, error) {
	if source.publishChannel != nil {
		return source.publishChannel, nil
	}

	err := ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("publish analysis failure: %w", err)
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
			"open scheduler rabbitmq analysis failure channel: %w",
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

func (source *AnalysisJobSource) resetPublisher() {
	if source.publishChannel == nil {
		return
	}

	channel := source.publishChannel

	source.publishChannel = nil
	source.publishConfirms = nil
	source.publishReturns = nil

	closeRabbitMQChannelAsync(channel)
}

type analysisJobReceipt struct {
	source   *AnalysisJobSource
	delivery amqp.Delivery
	job      contracts.AnalyzerJob
}

func (receipt *analysisJobReceipt) Job() contracts.AnalyzerJob {
	return receipt.job
}

func (receipt *analysisJobReceipt) Ack(context.Context) error {
	receipt.source.mutex.Lock()
	defer receipt.source.mutex.Unlock()

	return receipt.delivery.Ack(false)
}

func (receipt *analysisJobReceipt) Park(context.Context) error {
	receipt.source.mutex.Lock()
	defer receipt.source.mutex.Unlock()

	// Requeue onto the work queue without publishing to the failed queue, so the
	// job waits for the analyzer to recover instead of consuming retries.
	return receipt.delivery.Nack(false, true)
}

func (receipt *analysisJobReceipt) Retry(ctx context.Context, cause error) error {
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

var _ analysis.JobSource = (*AnalysisJobSource)(nil)
