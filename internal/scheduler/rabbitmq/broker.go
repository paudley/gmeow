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
	defaultQueuePrefix               = "gmeow."
	testQueuePrefix                  = "gmeow.test."
	workSuffix                       = "analysis.work"
	reconcileSuffix                  = "analysis.reconcile"
	retrySuffix                      = "analysis.retry.v2"
	failedSuffix                     = "analysis.failed"
	deadLetterSuffix                 = "analysis.dead"
	projectionSuffix                 = "projection.refresh"
	sourceImportWorkSuffix           = "source_import.work"
	sourceImportRetrySuffix          = "source_import.retry"
	sourceImportFailedSuffix         = "source_import.failed"
	sourceImportDeadLetterSuffix     = "source_import.dead"
	analysisSuffix                   = "analysis"
	projectionExSuffix               = "projection"
	sourceImportExSuffix             = "source_import"
	workRoutingKey                   = "analysis.work"
	retryRoutingKey                  = "analysis.retry"
	failedRoutingKey                 = "analysis.failed"
	deadLetterRoutingKey             = "analysis.dead"
	projectionRoutingKey             = "projection.refresh"
	sourceImportWorkRoutingKey       = "source_import.work"
	sourceImportRetryRoutingKey      = "source_import.retry"
	sourceImportFailedRoutingKey     = "source_import.failed"
	sourceImportDeadLetterRoutingKey = "source_import.dead"
)

type Config struct {
	URL          string
	QueuePrefix  string
	RetryLimit   int
	RetryBackoff time.Duration
	// Analyzers is the set of analyzer names that each get their own work and
	// retry queue, so a slow analyzer's backlog never blocks a fast one. Empty is
	// tolerated (no analysis work queues declared) for source-import-only callers.
	Analyzers []string
}

type Broker struct {
	connection *amqp.Connection
	config     Config
	topology   topology
}

type topology struct {
	analysisExchange            string
	projectionExchange          string
	sourceImportExchange        string
	prefix                      string
	analyzers                   []string
	reconcileQueue              string
	failedQueue                 string
	deadLetterQueue             string
	projectionQueue             string
	sourceImportWorkQueue       string
	sourceImportRetryQueue      string
	sourceImportFailedQueue     string
	sourceImportDeadLetterQueue string
}

// Per-analyzer queue and routing-key helpers. Work and retry are partitioned by
// analyzer; failed, dead-letter, and projection stay shared.
func (top topology) workQueueFor(analyzer string) string {
	return top.prefix + workSuffix + "." + analyzer
}

func (top topology) workRoutingKeyFor(analyzer string) string {
	return workRoutingKey + "." + analyzer
}

func (top topology) retryQueueFor(analyzer string) string {
	return top.prefix + retrySuffix + "." + analyzer
}

func (top topology) retryRoutingKeyFor(analyzer string) string {
	return retryRoutingKey + "." + analyzer
}

type queueBinding struct {
	queue      string
	routingKey string
	exchange   string
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
		topology:   newTopology(cfg.QueuePrefix, cfg.Analyzers),
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
	analyzers []config.AnalyzerConfig,
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
		Analyzers:    analyzerNames(analyzers),
	}
}

func analyzerNames(analyzers []config.AnalyzerConfig) []string {
	names := make([]string, 0, len(analyzers))
	for _, analyzer := range analyzers {
		name := strings.TrimSpace(analyzer.Name)
		if name != "" {
			names = append(names, name)
		}
	}

	return names
}

func TestConfigFromResolved(
	rabbit config.ResolvedRabbitMQ,
	scheduler config.ResolvedScheduler,
	analyzers []config.AnalyzerConfig,
) Config {
	cfg := ConfigFromResolved(rabbit, scheduler, analyzers)
	cfg.URL = rabbit.TestURL
	cfg.QueuePrefix = testQueuePrefix

	return cfg
}

func newTopology(prefix string, analyzers []string) topology {
	return topology{
		analysisExchange:            prefix + analysisSuffix,
		projectionExchange:          prefix + projectionExSuffix,
		sourceImportExchange:        prefix + sourceImportExSuffix,
		prefix:                      prefix,
		analyzers:                   append([]string(nil), analyzers...),
		reconcileQueue:              prefix + reconcileSuffix,
		failedQueue:                 prefix + failedSuffix,
		deadLetterQueue:             prefix + deadLetterSuffix,
		projectionQueue:             prefix + projectionSuffix,
		sourceImportWorkQueue:       prefix + sourceImportWorkSuffix,
		sourceImportRetryQueue:      prefix + sourceImportRetrySuffix,
		sourceImportFailedQueue:     prefix + sourceImportFailedSuffix,
		sourceImportDeadLetterQueue: prefix + sourceImportDeadLetterSuffix,
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

	if err := broker.declareSourceImportQueues(channel); err != nil {
		return err
	}

	bindings := []queueBinding{
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
	bindings = append(bindings, broker.sourceImportBindings()...)
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

	return broker.declareAnalyzerQueues(channel)
}

// declareAnalyzerQueues declares, per configured analyzer, a priority work queue
// (dead-lettering failures to the shared failed queue) and a retry queue whose
// expired messages dead-letter back to that analyzer's work queue. Partitioning
// work and retry by analyzer keeps a slow analyzer's backlog from blocking a fast
// one and preserves per-analyzer retry granularity.
func (broker *Broker) declareAnalyzerQueues(channel *amqp.Channel) error {
	for _, analyzer := range broker.topology.analyzers {
		workQueue := broker.topology.workQueueFor(analyzer)
		retryQueue := broker.topology.retryQueueFor(analyzer)

		if _, err := channel.QueueDeclare(workQueue, true, false, false, false, amqp.Table{
			"x-max-priority":            int32(100),
			"x-dead-letter-exchange":    broker.topology.analysisExchange,
			"x-dead-letter-routing-key": failedRoutingKey,
		}); err != nil {
			return fmt.Errorf("declare work queue %s: %w", workQueue, err)
		}

		if _, err := channel.QueueDeclare(retryQueue, true, false, false, false, amqp.Table{
			"x-dead-letter-exchange":    broker.topology.analysisExchange,
			"x-dead-letter-routing-key": broker.topology.workRoutingKeyFor(analyzer),
		}); err != nil {
			return fmt.Errorf("declare retry queue %s: %w", retryQueue, err)
		}

		if err := channel.QueueBind(
			workQueue,
			broker.topology.workRoutingKeyFor(analyzer),
			broker.topology.analysisExchange,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("bind work queue %s: %w", workQueue, err)
		}

		if err := channel.QueueBind(
			retryQueue,
			broker.topology.retryRoutingKeyFor(analyzer),
			broker.topology.analysisExchange,
			false,
			nil,
		); err != nil {
			return fmt.Errorf("bind retry queue %s: %w", retryQueue, err)
		}
	}

	return nil
}

func (broker *Broker) PurgeAll(ctx context.Context) (int, error) {
	channel, err := broker.channel(ctx)
	if err != nil {
		return 0, err
	}
	defer channel.Close()

	total := 0

	queues := []string{
		broker.topology.reconcileQueue,
		broker.topology.failedQueue,
		broker.topology.deadLetterQueue,
		broker.topology.projectionQueue,
	}
	for _, analyzer := range broker.topology.analyzers {
		queues = append(
			queues,
			broker.topology.workQueueFor(analyzer),
			broker.topology.retryQueueFor(analyzer),
		)
	}

	for _, queue := range queues {
		purged, err := channel.QueuePurge(queue, false)
		if err != nil {
			return total, fmt.Errorf("purge queue %s: %w", queue, err)
		}

		total += purged
	}

	return total, nil
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
		broker.topology.workRoutingKeyFor(job.Analyzer.Name),
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

	pending := 0
	retry := 0
	for _, analyzer := range broker.topology.analyzers {
		work, err := channel.QueueInspect(broker.topology.workQueueFor(analyzer))
		if err != nil {
			return contracts.SchedulerStatus{}, err
		}

		retryQ, err := channel.QueueInspect(broker.topology.retryQueueFor(analyzer))
		if err != nil {
			return contracts.SchedulerStatus{}, err
		}

		pending += work.Messages
		retry += retryQ.Messages
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
		Pending:       pending,
		Retry:         retry,
		Failed:        failed.Messages,
		DeadLetter:    dead.Messages,
	}, nil
}

// PerAnalyzerStatus returns the work and retry depth of each analyzer queue so an
// operator can see which analyzer is backed up.
func (broker *Broker) PerAnalyzerStatus(
	ctx context.Context,
) ([]contracts.AnalyzerQueueDepth, error) {
	channel, err := broker.channel(ctx)
	if err != nil {
		return nil, err
	}
	defer channel.Close()

	depths := make([]contracts.AnalyzerQueueDepth, 0, len(broker.topology.analyzers))
	for _, analyzer := range broker.topology.analyzers {
		work, err := channel.QueueInspect(broker.topology.workQueueFor(analyzer))
		if err != nil {
			return nil, err
		}

		retryQ, err := channel.QueueInspect(broker.topology.retryQueueFor(analyzer))
		if err != nil {
			return nil, err
		}

		depths = append(depths, contracts.AnalyzerQueueDepth{
			Analyzer: analyzer,
			Pending:  work.Messages,
			Retry:    retryQ.Messages,
		})
	}

	return depths, nil
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

func (broker *Broker) PendingJobs(
	ctx context.Context,
	limit int,
) ([]contracts.AnalyzerJob, error) {
	if limit <= 0 {
		limit = 20
	}

	jobs := []contracts.AnalyzerJob{}
	for _, analyzer := range broker.topology.analyzers {
		if len(jobs) >= limit {
			break
		}

		batch, err := broker.peekJobs(
			ctx,
			broker.topology.workQueueFor(analyzer),
			limit-len(jobs),
		)
		if err != nil {
			return nil, err
		}

		jobs = append(jobs, batch...)
	}

	return jobs, nil
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

	// Drain any scratch left by a crashed prior run, routing each job back to its
	// own analyzer's work queue.
	moved, err := broker.drainReconcileByAnalyzer(ctx, channel, confirms)
	if err != nil {
		return response, err
	}
	response.Republished += moved

	seen := map[string]bool{}
	for _, analyzer := range broker.topology.analyzers {
		workQueue := broker.topology.workQueueFor(analyzer)
		for response.Checked < limit {
			delivery, ok, err := channel.Get(workQueue, false)
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
	}

	moved, err = broker.drainReconcileByAnalyzer(ctx, channel, confirms)
	if err != nil {
		return response, err
	}
	response.Republished += moved

	return response, nil
}

// drainReconcileByAnalyzer moves every job buffered in the shared reconcile
// scratch queue back to its own analyzer's work queue, so kept jobs return to
// the correct partition.
func (broker *Broker) drainReconcileByAnalyzer(
	ctx context.Context,
	channel *amqp.Channel,
	confirms <-chan amqp.Confirmation,
) (int, error) {
	moved := 0
	for {
		delivery, ok, err := channel.Get(broker.topology.reconcileQueue, false)
		if err != nil {
			return moved, err
		}
		if !ok {
			break
		}

		var job contracts.AnalyzerJob
		if err := json.Unmarshal(delivery.Body, &job); err != nil {
			_ = delivery.Nack(false, true)

			return moved, err
		}

		if err := publishAndWaitConfirmed(
			ctx,
			channel,
			confirms,
			broker.topology.analysisExchange,
			broker.topology.workRoutingKeyFor(job.Analyzer.Name),
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
			broker.topology.workRoutingKeyFor(job.Analyzer.Name),
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

		routingKey := broker.topology.retryRoutingKeyFor(job.Analyzer.Name)
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

	// Retry queues are bound per analyzer; the aggregate "analysis.retry" key is
	// unroutable on this exchange and would silently drop the retry.
	routingKey := broker.topology.retryRoutingKeyFor(job.Analyzer.Name)
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
