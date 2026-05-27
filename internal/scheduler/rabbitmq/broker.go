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
	reconcileSuffix      = "analysis.reconcile"
	retrySuffix          = "analysis.retry.v2"
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
	QueuePrefix  string
	RetryLimit   int
	RetryBackoff time.Duration
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
	reconcileQueue     string
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
		reconcileQueue:     prefix + reconcileSuffix,
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
		broker.topology.reconcileQueue,
		true,
		false,
		false,
		false,
		amqp.Table{"x-max-priority": int32(100)},
	); err != nil {
		return fmt.Errorf("declare reconcile queue: %w", err)
	}

	if _, err := channel.QueueDeclare(
		broker.topology.retryQueue,
		true,
		false,
		false,
		false,
		amqp.Table{
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
		err := channel.QueueBind(
			binding.queue,
			binding.routingKey,
			binding.exchange,
			false,
			nil,
		)
		if err != nil {
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
	return broker.peekJobs(ctx, broker.topology.deadLetterQueue, limit)
}

func (broker *Broker) FailedJobs(
	ctx context.Context,
	limit int,
) ([]contracts.AnalyzerJob, error) {
	return broker.peekJobs(ctx, broker.topology.failedQueue, limit)
}

func (broker *Broker) ActiveJobKeys(
	ctx context.Context,
	limit int,
) (map[string]bool, error) {
	if limit <= 0 {
		limit = 100000
	}

	keys := map[string]bool{}
	for _, queue := range []string{broker.topology.workQueue, broker.topology.retryQueue} {
		if err := broker.peekJobKeys(ctx, queue, limit, keys); err != nil {
			return nil, err
		}
	}

	return keys, nil
}

func (broker *Broker) PendingJobs(
	ctx context.Context,
	limit int,
) ([]contracts.AnalyzerJob, error) {
	return broker.peekJobs(ctx, broker.topology.workQueue, limit)
}

func (broker *Broker) ReconcilePending(
	ctx context.Context,
	limit int,
	isSatisfied func(contracts.AnalyzerJob) (bool, error),
) (contracts.ReconcilePendingResponse, error) {
	if isSatisfied == nil {
		return contracts.ReconcilePendingResponse{}, errors.New(
			"reconcile predicate is required",
		)
	}
	if limit <= 0 {
		limit = 100
	}

	channel, err := broker.channel(ctx)
	if err != nil {
		return contracts.ReconcilePendingResponse{}, err
	}
	defer channel.Close()
	confirms, err := enablePublishConfirms(channel)
	if err != nil {
		return contracts.ReconcilePendingResponse{}, err
	}

	response := contracts.ReconcilePendingResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
	}
	if moved, err := broker.republishQueue(
		ctx,
		channel,
		confirms,
		broker.topology.reconcileQueue,
		broker.topology.analysisExchange,
		workRoutingKey,
		0,
	); err != nil {
		return response, err
	} else {
		response.Republished += moved
	}

	seen := map[string]bool{}
	for response.Checked < limit {
		delivery, ok, err := channel.Get(broker.topology.workQueue, false)
		if err != nil {
			return response, err
		}
		if !ok {
			break
		}

		var job contracts.AnalyzerJob
		if err := json.Unmarshal(delivery.Body, &job); err != nil {
			_ = delivery.Nack(false, true)

			return response, err
		}
		response.Checked++

		satisfied, err := isSatisfied(job)
		if err != nil {
			_ = delivery.Nack(false, true)

			return response, err
		}
		if satisfied {
			if err := delivery.Ack(false); err != nil {
				return response, err
			}
			response.DroppedSatisfied++

			continue
		}
		if seen[job.IdempotencyKey] {
			if err := delivery.Ack(false); err != nil {
				return response, err
			}
			response.DroppedDuplicate++

			continue
		}
		seen[job.IdempotencyKey] = true
		response.KeptJobs = append(response.KeptJobs, job)

		if err := publishAndWaitConfirmed(
			ctx,
			channel,
			confirms,
			"",
			broker.topology.reconcileQueue,
			publishingFromDelivery(delivery),
		); err != nil {
			_ = delivery.Nack(false, true)

			return response, err
		}
		if err := delivery.Ack(false); err != nil {
			return response, err
		}
		response.Kept++
	}

	if moved, err := broker.republishQueue(
		ctx,
		channel,
		confirms,
		broker.topology.reconcileQueue,
		broker.topology.analysisExchange,
		workRoutingKey,
		0,
	); err != nil {
		return response, err
	} else {
		response.Republished += moved
	}

	return response, nil
}

func (broker *Broker) peekJobKeys(
	ctx context.Context,
	queue string,
	limit int,
	keys map[string]bool,
) error {
	jobs, err := broker.peekJobs(ctx, queue, limit)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if job.IdempotencyKey != "" {
			keys[job.IdempotencyKey] = true
		}
	}

	return nil
}

func (broker *Broker) republishQueue(
	ctx context.Context,
	channel *amqp.Channel,
	confirms <-chan amqp.Confirmation,
	sourceQueue string,
	targetExchange string,
	targetRoutingKey string,
	limit int,
) (int, error) {
	moved := 0
	for limit <= 0 || moved < limit {
		delivery, ok, err := channel.Get(sourceQueue, false)
		if err != nil {
			return moved, err
		}
		if !ok {
			break
		}

		if err := publishAndWaitConfirmed(
			ctx,
			channel,
			confirms,
			targetExchange,
			targetRoutingKey,
			publishingFromDelivery(delivery),
		); err != nil {
			_ = delivery.Nack(false, true)

			return moved, err
		}
		if err := delivery.Ack(false); err != nil {
			return moved, err
		}

		moved++
	}

	return moved, nil
}

func publishingFromDelivery(delivery amqp.Delivery) amqp.Publishing {
	return amqp.Publishing{
		ContentType:     delivery.ContentType,
		ContentEncoding: delivery.ContentEncoding,
		DeliveryMode:    delivery.DeliveryMode,
		Priority:        delivery.Priority,
		CorrelationId:   delivery.CorrelationId,
		ReplyTo:         delivery.ReplyTo,
		Expiration:      delivery.Expiration,
		MessageId:       delivery.MessageId,
		Timestamp:       delivery.Timestamp,
		Type:            delivery.Type,
		UserId:          delivery.UserId,
		AppId:           delivery.AppId,
		Headers:         delivery.Headers,
		Body:            delivery.Body,
	}
}

func (broker *Broker) peekJobs(
	ctx context.Context,
	queue string,
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
		delivery, ok, err := channel.Get(queue, false)
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

func (broker *Broker) ProcessProjectionRefreshes(
	ctx context.Context,
	limit int,
	refresh scheduler.ProjectionRefreshFunc,
) (int, error) {
	if refresh == nil {
		return 0, errors.New("projection refresh function is required")
	}
	if limit <= 0 {
		limit = 100
	}

	channel, err := broker.channel(ctx)
	if err != nil {
		return 0, err
	}
	defer channel.Close()

	deliveries := []amqp.Delivery{}
	for len(deliveries) < limit {
		delivery, ok, err := channel.Get(broker.topology.projectionQueue, false)
		if err != nil {
			return len(deliveries), err
		}
		if !ok {
			break
		}

		deliveries = append(deliveries, delivery)
	}
	if len(deliveries) == 0 {
		return 0, nil
	}

	digests := make([]contracts.ObjectDigest, 0, len(deliveries))
	for _, delivery := range deliveries {
		digest, err := projectionRefreshDigest(delivery.Body)
		if err != nil {
			_ = delivery.Nack(false, false)

			return 0, err
		}
		digests = append(digests, digest)
	}

	if err := refresh(ctx, digests); err != nil {
		for _, delivery := range deliveries {
			_ = delivery.Nack(false, true)
		}

		return 0, err
	}

	for processed, delivery := range deliveries {
		if err := delivery.Ack(false); err != nil {
			return processed, err
		}
	}

	return len(deliveries), nil
}

func projectionRefreshDigest(body []byte) (contracts.ObjectDigest, error) {
	var message struct {
		Digest string `json:"digest"`
	}
	if err := json.Unmarshal(body, &message); err != nil {
		return "", err
	}
	digest := contracts.ObjectDigest(strings.TrimSpace(message.Digest))
	if digest == "" {
		return "", errors.New("projection refresh digest is required")
	}

	return digest, nil
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
	confirms, err := enablePublishConfirms(channel)
	if err != nil {
		return 0, err
	}

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

		if err := publishAndWaitConfirmed(
			ctx,
			channel,
			confirms,
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
	confirms, err := enablePublishConfirms(channel)
	if err != nil {
		return 0, err
	}

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

		publishing := broker.retryPublishing(job, body)
		if err := publishAndWaitConfirmed(
			ctx,
			channel,
			confirms,
			broker.topology.analysisExchange,
			routingKey,
			publishing,
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
		broker.retryPublishing(job, body),
	)
}

func (broker *Broker) retryPublishing(
	job contracts.AnalyzerJob,
	body []byte,
) amqp.Publishing {
	backoff := broker.retryBackoff(job.Attempt)
	headers := amqp.Table{
		"idempotency_key": job.IdempotencyKey,
		"attempt":         int32(job.Attempt),
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

func (broker *Broker) retryBackoff(attempt int) time.Duration {
	if attempt <= 0 {
		return broker.config.RetryBackoff
	}

	backoff := broker.config.RetryBackoff
	for range attempt - 1 {
		backoff *= 2
	}

	return backoff
}

func publishConfirmed(
	ctx context.Context,
	channel *amqp.Channel,
	exchange string,
	routingKey string,
	publishing amqp.Publishing,
) error {
	confirms, err := enablePublishConfirms(channel)
	if err != nil {
		return err
	}

	return publishAndWaitConfirmed(
		ctx,
		channel,
		confirms,
		exchange,
		routingKey,
		publishing,
	)
}

func enablePublishConfirms(channel *amqp.Channel) (<-chan amqp.Confirmation, error) {
	if err := channel.Confirm(false); err != nil {
		return nil, fmt.Errorf("enable publish confirms: %w", err)
	}

	return channel.NotifyPublish(make(chan amqp.Confirmation, 1)), nil
}

func publishAndWaitConfirmed(
	ctx context.Context,
	channel *amqp.Channel,
	confirms <-chan amqp.Confirmation,
	exchange string,
	routingKey string,
	publishing amqp.Publishing,
) error {
	err := channel.PublishWithContext(
		ctx,
		exchange,
		routingKey,
		false,
		false,
		publishing,
	)
	if err != nil {
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
