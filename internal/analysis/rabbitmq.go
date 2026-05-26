// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

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

const (
	defaultQueuePrefix = "gmeow."
	testQueuePrefix    = "gmeow.test."
	workQueueSuffix    = "analysis.work"
	failedRoutingKey   = "analysis.failed"
	analysisExchange   = "analysis"
)

type RabbitMQSourceConfig struct {
	URL         string
	QueuePrefix string
}

type RabbitMQSource struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	deliveries <-chan amqp.Delivery
	config     RabbitMQSourceConfig
	mutex      sync.Mutex
}

func NewRabbitMQSource(
	ctx context.Context,
	config RabbitMQSourceConfig,
) (*RabbitMQSource, error) {
	if strings.TrimSpace(config.URL) == "" {
		return nil, errors.New("analysis rabbitmq url is required")
	}

	if strings.TrimSpace(config.QueuePrefix) == "" {
		config.QueuePrefix = defaultQueuePrefix
	}

	if config.QueuePrefix != defaultQueuePrefix &&
		!strings.HasPrefix(config.QueuePrefix, testQueuePrefix) {
		return nil, errors.New("analysis rabbitmq queue prefix must be gmeow. or gmeow.test.")
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	connection, err := amqp.DialConfig(config.URL, amqp.Config{
		Properties: amqp.Table{"connection_name": "gmeow.analysis"},
	})
	if err != nil {
		return nil, fmt.Errorf("connect analysis rabbitmq: %w", err)
	}

	source := &RabbitMQSource{config: config, connection: connection}

	return source, nil
}

func (source *RabbitMQSource) Receive(ctx context.Context) (JobReceipt, error) {
	err := source.ensureConsumer(ctx)
	if err != nil {
		return nil, err
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case delivery, ok := <-source.deliveries:
		if !ok {
			return nil, errors.New("analysis rabbitmq delivery channel closed")
		}

		var job contracts.AnalyzerJob
		err := json.Unmarshal(delivery.Body, &job)
		if err != nil {
			_ = delivery.Nack(false, false)

			return nil, fmt.Errorf("decode analysis job: %w", err)
		}

		return &rabbitMQReceipt{source: source, delivery: delivery, job: job}, nil
	}
}

func (source *RabbitMQSource) Close() error {
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

func (source *RabbitMQSource) ensureConsumer(ctx context.Context) error {
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
		return fmt.Errorf("open analysis rabbitmq channel: %w", err)
	}

	if err := channel.Qos(1, 0, false); err != nil {
		_ = channel.Close()

		return fmt.Errorf("set analysis rabbitmq qos: %w", err)
	}

	deliveries, err := channel.ConsumeWithContext(
		ctx,
		source.config.QueuePrefix+workQueueSuffix,
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

func (source *RabbitMQSource) publishFailure(
	ctx context.Context,
	job contracts.AnalyzerJob,
	cause error,
) error {
	body, err := json.Marshal(job)
	if err != nil {
		return err
	}

	channel, err := source.connection.Channel()
	if err != nil {
		return fmt.Errorf("open analysis failure channel: %w", err)
	}
	defer channel.Close()

	if err := channel.Confirm(false); err != nil {
		return fmt.Errorf("enable analysis failure confirms: %w", err)
	}

	confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))

	headers := amqp.Table{"attempt": int32(job.Attempt)}
	if cause != nil {
		headers["failure"] = cause.Error()
	}

	if err := channel.PublishWithContext(
		ctx,
		source.config.QueuePrefix+analysisExchange,
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

type rabbitMQReceipt struct {
	job      contracts.AnalyzerJob
	source   *RabbitMQSource
	delivery amqp.Delivery
}

func (receipt *rabbitMQReceipt) Job() contracts.AnalyzerJob {
	return receipt.job
}

func (receipt *rabbitMQReceipt) Ack(context.Context) error {
	return receipt.delivery.Ack(false)
}

func (receipt *rabbitMQReceipt) Retry(ctx context.Context, cause error) error {
	err := receipt.source.publishFailure(ctx, receipt.job, cause)
	if err != nil {
		_ = receipt.delivery.Nack(false, true)

		return err
	}

	return receipt.delivery.Ack(false)
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

var _ JobSource = (*RabbitMQSource)(nil)
