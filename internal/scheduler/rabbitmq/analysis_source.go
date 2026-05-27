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
	Prefetch    int
}

type AnalysisJobSource struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	deliveries <-chan amqp.Delivery
	config     AnalysisJobSourceConfig
	mutex      sync.Mutex
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

func (source *AnalysisJobSource) ensureConsumer(ctx context.Context) error {
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
		return fmt.Errorf("open scheduler rabbitmq analysis channel: %w", err)
	}

	prefetch := source.config.Prefetch
	if prefetch <= 0 {
		prefetch = 1
	}
	if err := channel.Qos(prefetch, 0, false); err != nil {
		_ = channel.Close()

		return fmt.Errorf("set scheduler rabbitmq analysis qos: %w", err)
	}

	deliveries, err := channel.ConsumeWithContext(
		ctx,
		source.config.QueuePrefix+workSuffix,
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

	channel, err := source.connection.Channel()
	if err != nil {
		return fmt.Errorf("open scheduler rabbitmq analysis failure channel: %w", err)
	}
	defer channel.Close()

	if err := channel.Confirm(false); err != nil {
		return fmt.Errorf("enable scheduler rabbitmq analysis failure confirms: %w", err)
	}

	confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))

	headers := amqp.Table{"attempt": int32(job.Attempt)}
	if cause != nil {
		headers["failure"] = cause.Error()
	}

	if err := channel.PublishWithContext(
		ctx,
		source.config.QueuePrefix+analysisSuffix,
		failedRoutingKey,
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
			return errors.New("analysis failure publish was not confirmed")
		}

		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type analysisJobReceipt struct {
	job      contracts.AnalyzerJob
	source   *AnalysisJobSource
	delivery amqp.Delivery
}

func (receipt *analysisJobReceipt) Job() contracts.AnalyzerJob {
	return receipt.job
}

func (receipt *analysisJobReceipt) Ack(context.Context) error {
	receipt.source.mutex.Lock()
	defer receipt.source.mutex.Unlock()

	return receipt.delivery.Ack(false)
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
