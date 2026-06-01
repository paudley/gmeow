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
	rabbitMQChannelOpenTimeout       = 2 * time.Second
	rabbitMQPublishConfirmTimeout    = 5 * time.Second
	rabbitMQChannelOpenAttempts      = 2
	rabbitMQStreamPrefetch           = 1000
)

var (
	errBrokerClosed             = errors.New("rabbitmq broker is closed")
	errObjectChangeFuncRequired = errors.New("object change function is required")
	errConsumerStreamClosed     = errors.New("rabbitmq consumer stream closed")
	errRabbitMQPublishReturned  = errors.New("rabbitmq publish returned")
	errRabbitMQPublishNacked    = errors.New("rabbitmq publish negatively acknowledged")
	errRabbitMQConfirmClosed    = errors.New("rabbitmq publish confirm channel closed")
	errRabbitMQConfirmTimeout   = errors.New("rabbitmq publish confirm timeout")
)

type Config struct {
	URL          string
	QueuePrefix  string
	Analyzers    []string
	RetryLimit   int
	RetryBackoff time.Duration
}

type Broker struct {
	connection      *amqp.Connection
	publishChannel  *amqp.Channel
	publishConfirms <-chan amqp.Confirmation
	publishReturns  <-chan amqp.Return
	consumeStreams  map[string]*rabbitMQConsumerStream
	adminChannel    *amqp.Channel
	topology        topology
	config          Config
	mu              sync.Mutex
	publishMu       sync.Mutex
	consumeMu       sync.Mutex
	adminMu         sync.Mutex
}

type rabbitMQConsumerStream struct {
	channel    *amqp.Channel
	deliveries <-chan amqp.Delivery
}

type channelOpenResult struct {
	channel *amqp.Channel
	err     error
}

type topology struct {
	failedQueue                 string
	projectionExchange          string
	sourceImportExchange        string
	prefix                      string
	reconcileQueue              string
	analysisExchange            string
	deadLetterQueue             string
	projectionQueue             string
	sourceImportWorkQueue       string
	sourceImportRetryQueue      string
	sourceImportFailedQueue     string
	sourceImportDeadLetterQueue string
	analyzers                   []string
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

	conn, err := dialRabbitMQ(cfg)
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

func dialRabbitMQ(cfg Config) (*amqp.Connection, error) {
	conn, err := amqp.DialConfig(cfg.URL, amqp.Config{
		Properties: amqp.Table{"connection_name": "gmeow.scheduler"},
	})
	if err != nil {
		return nil, fmt.Errorf("connect rabbitmq: %w", err)
	}

	return conn, nil
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
	channel, err := broker.admin(ctx)
	if err != nil {
		return err
	}

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
		err = channel.QueueBind(
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

		err := channel.QueueBind(
			workQueue,
			broker.topology.workRoutingKeyFor(analyzer),
			broker.topology.analysisExchange,
			false,
			nil,
		)
		if err != nil {
			return fmt.Errorf("bind work queue %s: %w", workQueue, err)
		}

		err = channel.QueueBind(
			retryQueue,
			broker.topology.retryRoutingKeyFor(analyzer),
			broker.topology.analysisExchange,
			false,
			nil,
		)
		if err != nil {
			return fmt.Errorf("bind retry queue %s: %w", retryQueue, err)
		}
	}

	return nil
}

func (broker *Broker) PurgeAll(ctx context.Context) (int, error) {
	total := 0

	queues := []string{
		broker.topology.reconcileQueue,
		broker.topology.failedQueue,
		broker.topology.deadLetterQueue,
		broker.topology.projectionQueue,
		broker.topology.sourceImportWorkQueue,
		broker.topology.sourceImportRetryQueue,
		broker.topology.sourceImportFailedQueue,
		broker.topology.sourceImportDeadLetterQueue,
	}
	for _, analyzer := range broker.topology.analyzers {
		queues = append(
			queues,
			broker.topology.workQueueFor(analyzer),
			broker.topology.retryQueueFor(analyzer),
		)
	}

	err := broker.withAdmin(ctx, func(channel *amqp.Channel) error {
		for _, queue := range queues {
			purged, err := channel.QueuePurge(queue, false)
			if err != nil {
				return fmt.Errorf("purge queue %s: %w", queue, err)
			}

			total += purged
		}

		return nil
	})
	if err != nil {
		return total, err
	}

	return total, nil
}

func (broker *Broker) Publish(ctx context.Context, job contracts.AnalyzerJob) error {
	body, err := json.Marshal(job)
	if err != nil {
		return err
	}

	return broker.publishPersistent(
		ctx,
		broker.topology.analysisExchange,
		broker.topology.workRoutingKeyFor(job.Analyzer.Name),
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    job.IdempotencyKey,
			Timestamp:    time.Now().UTC(),
			Priority:     clampPriority(job.Priority),
			Headers: amqp.Table{
				"idempotency_key": job.IdempotencyKey,
				"attempt":         int32(job.Attempt),
			},
			Body: body,
		},
	)
}

func (broker *Broker) PublishObjectChanges(
	ctx context.Context,
	request contracts.ObjectChangeRequest,
) error {
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}

	messageID := "object-change:" + time.Now().UTC().Format(time.RFC3339Nano)
	if len(request.ObjectDigests) == 1 {
		messageID = "object-change:" + string(request.ObjectDigests[0])
	}

	return broker.publishPersistent(
		ctx,
		broker.topology.projectionExchange,
		projectionRoutingKey,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    messageID,
			Timestamp:    time.Now().UTC(),
			Body:         body,
		},
	)
}

func (broker *Broker) PublishProjectionRefresh(
	ctx context.Context,
	digest contracts.ObjectDigest,
) error {
	return broker.PublishObjectChanges(ctx, contracts.ObjectChangeRequest{
		SchemaVersion:  contracts.SchemaVersionPhase00,
		ObjectDigests:  []contracts.ObjectDigest{digest},
		RequestedBy:    "scheduler",
		Reason:         "projection_refresh",
		ProjectionOnly: true,
	})
}

func (broker *Broker) Status(ctx context.Context) (contracts.SchedulerStatus, error) {
	pending := 0
	retry := 0

	var failed, dead amqp.Queue

	err := broker.withAdmin(ctx, func(channel *amqp.Channel) error {
		var inspectErr error

		for _, analyzer := range broker.topology.analyzers {
			workQueue := broker.topology.workQueueFor(analyzer)

			work, err := inspectQueue(channel, workQueue)
			if err != nil {
				return err
			}

			retryQueue := broker.topology.retryQueueFor(analyzer)

			retryQ, err := inspectQueue(channel, retryQueue)
			if err != nil {
				return err
			}

			pending += work.Messages
			retry += retryQ.Messages
		}

		failed, inspectErr = inspectQueue(channel, broker.topology.failedQueue)
		if inspectErr != nil {
			return inspectErr
		}

		dead, inspectErr = inspectQueue(channel, broker.topology.deadLetterQueue)
		if inspectErr != nil {
			return inspectErr
		}

		return nil
	})
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
	depths := make([]contracts.AnalyzerQueueDepth, 0, len(broker.topology.analyzers))

	err := broker.withAdmin(ctx, func(channel *amqp.Channel) error {
		for _, analyzer := range broker.topology.analyzers {
			workQueue := broker.topology.workQueueFor(analyzer)

			work, err := inspectQueue(channel, workQueue)
			if err != nil {
				return err
			}

			retryQueue := broker.topology.retryQueueFor(analyzer)

			retryQ, err := inspectQueue(channel, retryQueue)
			if err != nil {
				return err
			}

			depths = append(depths, contracts.AnalyzerQueueDepth{
				Analyzer: analyzer,
				Pending:  work.Messages,
				Retry:    retryQ.Messages,
			})
		}

		return nil
	})
	if err != nil {
		return nil, err
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

	response := contracts.ReconcilePendingResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
	}

	consumeErr := broker.withConsumer(ctx, func(channel *amqp.Channel) error {
		// Drain any scratch left by a crashed prior run, routing each job back to its
		// own analyzer's work queue.
		moved, drainErr := broker.drainReconcileByAnalyzer(ctx, channel)
		if drainErr != nil {
			return drainErr
		}

		response.Republished += moved

		seen := map[string]bool{}

		for _, analyzer := range broker.topology.analyzers {
			workQueue := broker.topology.workQueueFor(analyzer)

			for response.Checked < limit {
				delivery, received, getErr := broker.receiveDelivery(
					ctx,
					workQueue,
					false,
				)
				if getErr != nil {
					return getErr
				}

				if !received {
					break
				}

				var job contracts.AnalyzerJob

				unmarshalErr := json.Unmarshal(delivery.Body, &job)
				if unmarshalErr != nil {
					return nackAfterError(
						delivery,
						fmt.Errorf("decode reconcile job: %w", unmarshalErr),
					)
				}

				response.Checked++

				satisfied, satisfyErr := isSatisfied(job)
				if satisfyErr != nil {
					return nackAfterError(delivery, satisfyErr)
				}

				if satisfied {
					ackErr := ackDelivery(delivery)
					if ackErr != nil {
						return ackErr
					}

					response.DroppedSatisfied++

					continue
				}

				if seen[job.IdempotencyKey] {
					ackErr := ackDelivery(delivery)
					if ackErr != nil {
						return ackErr
					}

					response.DroppedDuplicate++

					continue
				}

				seen[job.IdempotencyKey] = true
				response.KeptJobs = append(response.KeptJobs, job)

				publishErr := broker.publishPersistent(
					ctx,
					"",
					broker.topology.reconcileQueue,
					publishingFromDelivery(delivery),
				)
				if publishErr != nil {
					return nackAfterError(delivery, publishErr)
				}

				ackErr := ackDelivery(delivery)
				if ackErr != nil {
					return ackErr
				}

				response.Kept++
			}
		}

		moved, drainErr = broker.drainReconcileByAnalyzer(ctx, channel)
		if drainErr != nil {
			return drainErr
		}

		response.Republished += moved

		return nil
	})
	if consumeErr != nil {
		return response, consumeErr
	}

	return response, nil
}

// drainReconcileByAnalyzer moves every job buffered in the shared reconcile
// scratch queue back to its own analyzer's work queue, so kept jobs return to
// the correct partition.
func (broker *Broker) drainReconcileByAnalyzer(
	ctx context.Context,
	channel *amqp.Channel,
) (int, error) {
	moved := 0

	for {
		delivery, received, err := broker.receiveDelivery(
			ctx,
			broker.topology.reconcileQueue,
			false,
		)
		if err != nil {
			return moved, err
		}

		if !received {
			break
		}

		var job contracts.AnalyzerJob

		unmarshalErr := json.Unmarshal(delivery.Body, &job)
		if unmarshalErr != nil {
			return moved, nackAfterError(
				delivery,
				fmt.Errorf("decode reconcile job: %w", unmarshalErr),
			)
		}

		publishErr := broker.publishPersistent(
			ctx,
			broker.topology.analysisExchange,
			broker.topology.workRoutingKeyFor(job.Analyzer.Name),
			publishingFromDelivery(delivery),
		)
		if publishErr != nil {
			return moved, nackAfterError(delivery, publishErr)
		}

		ackErr := ackDelivery(delivery)
		if ackErr != nil {
			return moved, ackErr
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

func inspectQueue(channel *amqp.Channel, queue string) (amqp.Queue, error) {
	declared, err := channel.QueueDeclarePassive(
		queue,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return amqp.Queue{}, fmt.Errorf("inspect queue %s: %w", queue, err)
	}

	return declared, nil
}

func ackDelivery(delivery amqp.Delivery) error {
	err := delivery.Ack(false)
	if err != nil {
		return fmt.Errorf("ack rabbitmq delivery: %w", err)
	}

	return nil
}

func nackDelivery(delivery amqp.Delivery, requeue bool) error {
	err := delivery.Nack(false, requeue)
	if err != nil {
		return fmt.Errorf("nack rabbitmq delivery: %w", err)
	}

	return nil
}

func nackAfterError(delivery amqp.Delivery, cause error) error {
	return errors.Join(cause, nackDelivery(delivery, true))
}

func (broker *Broker) peekJobs(
	ctx context.Context,
	queue string,
	limit int,
) ([]contracts.AnalyzerJob, error) {
	if limit <= 0 {
		limit = 20
	}

	jobs := []contracts.AnalyzerJob{}
	deliveries := []amqp.Delivery{}

	stream, err := broker.openConsumerStream(ctx, queue, "gmeow.peek."+queue)
	if err != nil {
		return nil, err
	}
	defer closeRabbitMQChannelAsync(stream.channel)

	for len(jobs) < limit {
		delivery, ok, err := receiveDeliveryFrom(ctx, queue, stream.deliveries, false)
		if err != nil {
			return nil, err
		}

		if !ok {
			break
		}

		deliveries = append(deliveries, delivery)

		var job contracts.AnalyzerJob

		unmarshalErr := json.Unmarshal(delivery.Body, &job)
		if unmarshalErr != nil {
			return nil, errors.Join(
				fmt.Errorf("decode queued analyzer job: %w", unmarshalErr),
				nackDeliveries(deliveries, true),
			)
		}

		jobs = append(jobs, job)
	}

	for _, delivery := range deliveries {
		nackErr := nackDelivery(delivery, true)
		if nackErr != nil {
			return nil, nackErr
		}
	}

	return jobs, nil
}

func (broker *Broker) ProcessObjectChanges(
	ctx context.Context,
	limit int,
	process scheduler.ObjectChangeFunc,
) (int, error) {
	if process == nil {
		return 0, errObjectChangeFuncRequired
	}

	if limit <= 0 {
		limit = 100
	}

	var processed int

	err := broker.withConsumer(ctx, func(channel *amqp.Channel) error {
		deliveries, readErr := broker.objectChangeDeliveries(
			ctx,
			broker.topology.projectionQueue,
			limit,
		)
		if readErr != nil {
			processed = len(deliveries)

			return fmt.Errorf("read object change deliveries: %w", readErr)
		}

		if len(deliveries) == 0 {
			return nil
		}

		requests, requestErr := objectChangeRequests(deliveries)
		if requestErr != nil {
			nackErr := nackDeliveries(deliveries, false)
			if nackErr != nil {
				return errors.Join(requestErr, nackErr)
			}

			return requestErr
		}

		processErr := process(ctx, requests)
		if processErr != nil {
			nackErr := nackDeliveries(deliveries, true)
			if nackErr != nil {
				return errors.Join(processErr, nackErr)
			}

			return fmt.Errorf("process object changes: %w", processErr)
		}

		acked, ackErr := ackDeliveries(deliveries)
		processed = acked

		return ackErr
	})
	if err != nil {
		return processed, err
	}

	return processed, nil
}

func objectChangeRequests(
	deliveries []amqp.Delivery,
) ([]contracts.ObjectChangeRequest, error) {
	requests := make([]contracts.ObjectChangeRequest, 0, len(deliveries))

	for _, delivery := range deliveries {
		request, err := objectChangeRequest(delivery.Body)
		if err != nil {
			return nil, err
		}

		requests = append(requests, request)
	}

	return requests, nil
}

func ackDeliveries(deliveries []amqp.Delivery) (int, error) {
	for processed, delivery := range deliveries {
		err := delivery.Ack(false)
		if err != nil {
			return processed, fmt.Errorf("ack object change delivery: %w", err)
		}
	}

	return len(deliveries), nil
}

func nackDeliveries(deliveries []amqp.Delivery, requeue bool) error {
	var joined error

	for _, delivery := range deliveries {
		err := delivery.Nack(false, requeue)
		if err != nil {
			joined = errors.Join(
				joined,
				fmt.Errorf("nack object change delivery: %w", err),
			)
		}
	}

	return joined
}

func objectChangeRequest(body []byte) (contracts.ObjectChangeRequest, error) {
	var request contracts.ObjectChangeRequest

	err := json.Unmarshal(body, &request)
	if err != nil {
		return contracts.ObjectChangeRequest{}, fmt.Errorf(
			"decode object change request: %w",
			err,
		)
	}

	if len(request.ObjectDigests) > 0 {
		request.SchemaVersion = contracts.SchemaVersionPhase00

		return request, nil
	}

	digest, err := projectionRefreshDigest(body)
	if err != nil {
		return contracts.ObjectChangeRequest{}, err
	}

	return contracts.ObjectChangeRequest{
		SchemaVersion:  contracts.SchemaVersionPhase00,
		ObjectDigests:  []contracts.ObjectDigest{digest},
		RequestedBy:    "scheduler",
		Reason:         "projection_refresh",
		ProjectionOnly: true,
	}, nil
}

func projectionRefreshDigest(body []byte) (contracts.ObjectDigest, error) {
	var message struct {
		Digest string `json:"digest"`
	}

	err := json.Unmarshal(body, &message)
	if err != nil {
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

	requeued := 0

	consumeErr := broker.withConsumer(ctx, func(channel *amqp.Channel) error {
		for requeued < limit {
			delivery, received, getErr := broker.receiveDelivery(
				ctx,
				broker.topology.deadLetterQueue,
				false,
			)
			if getErr != nil {
				return getErr
			}

			if !received {
				break
			}

			var job contracts.AnalyzerJob

			unmarshalErr := json.Unmarshal(delivery.Body, &job)
			if unmarshalErr != nil {
				return nackAfterError(
					delivery,
					fmt.Errorf("decode dead-letter job: %w", unmarshalErr),
				)
			}

			job.Attempt = 0

			body, marshalErr := json.Marshal(job)
			if marshalErr != nil {
				return nackAfterError(
					delivery,
					fmt.Errorf("encode requeued dead-letter job: %w", marshalErr),
				)
			}

			publishErr := broker.publishPersistent(
				ctx,
				broker.topology.analysisExchange,
				broker.topology.workRoutingKeyFor(job.Analyzer.Name),
				amqp.Publishing{
					ContentType:  "application/json",
					DeliveryMode: amqp.Persistent,
					MessageId:    job.IdempotencyKey,
					Timestamp:    time.Now().UTC(),
					Priority:     clampPriority(job.Priority),
					Body:         body,
				},
			)
			if publishErr != nil {
				return nackAfterError(delivery, publishErr)
			}

			ackErr := ackDelivery(delivery)
			if ackErr != nil {
				return ackErr
			}

			requeued++
		}

		return nil
	})
	if consumeErr != nil {
		return requeued, consumeErr
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

	processed := 0

	consumeErr := broker.withConsumer(ctx, func(channel *amqp.Channel) error {
		for processed < limit {
			delivery, received, getErr := broker.receiveDelivery(
				ctx,
				broker.topology.failedQueue,
				processed == 0,
			)
			if getErr != nil {
				return getErr
			}

			if !received {
				break
			}

			var job contracts.AnalyzerJob

			unmarshalErr := json.Unmarshal(delivery.Body, &job)
			if unmarshalErr != nil {
				return nackAfterError(
					delivery,
					fmt.Errorf("decode failed analyzer job: %w", unmarshalErr),
				)
			}

			job.Attempt++

			routingKey := broker.topology.retryRoutingKeyFor(job.Analyzer.Name)
			if job.Attempt > broker.config.RetryLimit {
				routingKey = deadLetterRoutingKey
			}

			body, marshalErr := json.Marshal(job)
			if marshalErr != nil {
				return nackAfterError(
					delivery,
					fmt.Errorf("encode retried analyzer job: %w", marshalErr),
				)
			}

			publishing := broker.retryPublishing(job, body)

			publishErr := broker.publishPersistent(
				ctx,
				broker.topology.analysisExchange,
				routingKey,
				publishing,
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

func (broker *Broker) RouteFailure(
	ctx context.Context,
	job contracts.AnalyzerJob,
) error {
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

	return broker.publishPersistent(
		ctx,
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
		Priority:     clampPriority(job.Priority),
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

func (broker *Broker) Close() error {
	var joined error

	broker.publishMu.Lock()
	if broker.publishChannel != nil {
		err := broker.publishChannel.Close()
		if err != nil {
			joined = errors.Join(joined, fmt.Errorf("close rabbitmq publisher: %w", err))
		}

		broker.publishChannel = nil
		broker.publishConfirms = nil
		broker.publishReturns = nil
	}
	broker.publishMu.Unlock()

	broker.consumeMu.Lock()
	for queue, stream := range broker.consumeStreams {
		err := stream.channel.Close()
		if err != nil {
			joined = errors.Join(
				joined,
				fmt.Errorf("close rabbitmq consumer %s: %w", queue, err),
			)
		}
	}

	broker.consumeStreams = nil
	broker.consumeMu.Unlock()

	broker.adminMu.Lock()
	if broker.adminChannel != nil {
		err := broker.adminChannel.Close()
		if err != nil {
			joined = errors.Join(joined, fmt.Errorf("close rabbitmq admin: %w", err))
		}

		broker.adminChannel = nil
	}
	broker.adminMu.Unlock()

	broker.mu.Lock()
	defer broker.mu.Unlock()

	if broker.connection == nil {
		return joined
	}

	err := broker.connection.Close()
	broker.connection = nil

	if err != nil {
		joined = errors.Join(joined, fmt.Errorf("close rabbitmq connection: %w", err))
	}

	return joined
}

func (broker *Broker) receiveDelivery(
	ctx context.Context,
	queue string,
	wait bool,
) (amqp.Delivery, bool, error) {
	deliveries, err := broker.consumerStream(ctx, queue)
	if err != nil {
		return amqp.Delivery{}, false, err
	}

	delivery, received, err := receiveDeliveryFrom(ctx, queue, deliveries, wait)
	if errors.Is(err, errConsumerStreamClosed) {
		broker.resetConsumerStream(queue)
	}

	return delivery, received, err
}

func receiveDeliveryFrom(
	ctx context.Context,
	queue string,
	deliveries <-chan amqp.Delivery,
	wait bool,
) (amqp.Delivery, bool, error) {
	if wait {
		select {
		case delivery, received := <-deliveries:
			if !received {
				return amqp.Delivery{}, false, fmt.Errorf(
					"%w: %s",
					errConsumerStreamClosed,
					queue,
				)
			}

			return delivery, true, nil
		case <-ctx.Done():
			return amqp.Delivery{}, false, fmt.Errorf(
				"receive queue %s: %w",
				queue,
				ctx.Err(),
			)
		}
	}

	select {
	case delivery, received := <-deliveries:
		if !received {
			return amqp.Delivery{}, false, fmt.Errorf(
				"%w: %s",
				errConsumerStreamClosed,
				queue,
			)
		}

		return delivery, true, nil
	default:
		return amqp.Delivery{}, false, nil
	}
}

func (broker *Broker) consumerStream(
	ctx context.Context,
	queue string,
) (<-chan amqp.Delivery, error) {
	broker.consumeMu.Lock()
	defer broker.consumeMu.Unlock()

	if broker.consumeStreams != nil {
		if stream, ok := broker.consumeStreams[queue]; ok {
			return stream.deliveries, nil
		}
	}

	stream, err := broker.openConsumerStream(ctx, queue, "gmeow."+queue)
	if err != nil {
		return nil, err
	}

	if broker.consumeStreams == nil {
		broker.consumeStreams = map[string]*rabbitMQConsumerStream{}
	}

	broker.consumeStreams[queue] = stream

	return stream.deliveries, nil
}

func (broker *Broker) openConsumerStream(
	ctx context.Context,
	queue string,
	consumerTag string,
) (*rabbitMQConsumerStream, error) {
	channel, err := broker.channel(ctx)
	if err != nil {
		return nil, err
	}

	qosErr := channel.Qos(rabbitMQStreamPrefetch, 0, false)
	if qosErr != nil {
		_ = channel.Close()

		return nil, fmt.Errorf("set rabbitmq consumer qos for queue %s: %w", queue, qosErr)
	}

	deliveries, err := channel.Consume(
		queue,
		consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = channel.Close()

		return nil, fmt.Errorf("consume queue %s: %w", queue, err)
	}

	return &rabbitMQConsumerStream{
		channel:    channel,
		deliveries: deliveries,
	}, nil
}

func (broker *Broker) resetConsumerStream(queue string) {
	broker.consumeMu.Lock()
	defer broker.consumeMu.Unlock()

	if broker.consumeStreams == nil {
		return
	}

	stream, ok := broker.consumeStreams[queue]
	if !ok {
		return
	}

	delete(broker.consumeStreams, queue)
	closeRabbitMQChannelAsync(stream.channel)
}

func (broker *Broker) objectChangeDeliveries(
	ctx context.Context,
	queue string,
	limit int,
) ([]amqp.Delivery, error) {
	deliveries := []amqp.Delivery{}

	for len(deliveries) < limit {
		delivery, received, err := broker.receiveDelivery(
			ctx,
			queue,
			len(deliveries) == 0,
		)
		if err != nil {
			return deliveries, fmt.Errorf("receive object change delivery: %w", err)
		}

		if !received {
			break
		}

		deliveries = append(deliveries, delivery)
	}

	return deliveries, nil
}

func (broker *Broker) publishPersistent(
	ctx context.Context,
	exchange string,
	routingKey string,
	publishing amqp.Publishing,
) error {
	broker.publishMu.Lock()
	defer broker.publishMu.Unlock()

	channel, err := broker.publisher(ctx)
	if err != nil {
		return err
	}

	err = channel.PublishWithContext(ctx, exchange, routingKey, true, false, publishing)
	if err != nil {
		broker.resetPublisher()

		return fmt.Errorf("publish rabbitmq message: %w", err)
	}

	err = waitRabbitMQPublishConfirmed(
		ctx,
		exchange,
		routingKey,
		broker.publishConfirms,
		broker.publishReturns,
	)
	if err != nil {
		broker.resetPublisher()

		return err
	}

	return nil
}

func (broker *Broker) publisher(
	ctx context.Context,
) (*amqp.Channel, error) {
	if broker.publishChannel != nil {
		return broker.publishChannel, nil
	}

	channel, err := broker.channel(ctx)
	if err != nil {
		return nil, err
	}

	confirms, returns, err := configureRabbitMQPublisher(channel)
	if err != nil {
		_ = channel.Close()

		return nil, err
	}

	broker.publishChannel = channel
	broker.publishConfirms = confirms
	broker.publishReturns = returns

	return channel, nil
}

func (broker *Broker) resetPublisher() {
	if broker.publishChannel == nil {
		return
	}

	channel := broker.publishChannel

	broker.publishChannel = nil
	broker.publishConfirms = nil
	broker.publishReturns = nil

	closeRabbitMQChannelAsync(channel)
}

func configureRabbitMQPublisher(
	channel *amqp.Channel,
) (<-chan amqp.Confirmation, <-chan amqp.Return, error) {
	err := channel.Confirm(false)
	if err != nil {
		return nil, nil, fmt.Errorf("enable rabbitmq publisher confirms: %w", err)
	}

	return channel.NotifyPublish(make(chan amqp.Confirmation, 1)),
		channel.NotifyReturn(make(chan amqp.Return, 1)),
		nil
}

func waitRabbitMQPublishConfirmed(
	ctx context.Context,
	exchange string,
	routingKey string,
	confirms <-chan amqp.Confirmation,
	returns <-chan amqp.Return,
) error {
	timer := time.NewTimer(rabbitMQPublishConfirmTimeout)
	defer timer.Stop()

	for {
		select {
		case returned, ok := <-returns:
			if !ok {
				return fmt.Errorf(
					"%w: exchange=%q routing_key=%q",
					errRabbitMQConfirmClosed,
					exchange,
					routingKey,
				)
			}

			return fmt.Errorf(
				"%w: exchange=%q routing_key=%q reply=%s",
				errRabbitMQPublishReturned,
				exchange,
				routingKey,
				returned.ReplyText,
			)
		case confirmation, ok := <-confirms:
			if !ok {
				return fmt.Errorf(
					"%w: exchange=%q routing_key=%q",
					errRabbitMQConfirmClosed,
					exchange,
					routingKey,
				)
			}

			if !confirmation.Ack {
				return fmt.Errorf(
					"%w: exchange=%q routing_key=%q",
					errRabbitMQPublishNacked,
					exchange,
					routingKey,
				)
			}

			return nil
		case <-ctx.Done():
			return fmt.Errorf("wait rabbitmq publish confirm: %w", ctx.Err())
		case <-timer.C:
			return fmt.Errorf(
				"%w: exchange=%q routing_key=%q",
				errRabbitMQConfirmTimeout,
				exchange,
				routingKey,
			)
		}
	}
}

func (broker *Broker) withConsumer(
	ctx context.Context,
	operation func(*amqp.Channel) error,
) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("use rabbitmq consumer: %w", err)
	}

	return operation(nil)
}

func (broker *Broker) withAdmin(
	ctx context.Context,
	operation func(*amqp.Channel) error,
) error {
	broker.adminMu.Lock()
	defer broker.adminMu.Unlock()

	channel, err := broker.admin(ctx)
	if err != nil {
		return err
	}

	err = operation(channel)
	if err != nil {
		broker.resetAdmin()

		return err
	}

	return nil
}

func (broker *Broker) admin(ctx context.Context) (*amqp.Channel, error) {
	if broker.adminChannel != nil {
		return broker.adminChannel, nil
	}

	channel, err := broker.channel(ctx)
	if err != nil {
		return nil, err
	}

	broker.adminChannel = channel

	return channel, nil
}

func (broker *Broker) resetAdmin() {
	if broker.adminChannel == nil {
		return
	}

	channel := broker.adminChannel

	broker.adminChannel = nil

	closeRabbitMQChannelAsync(channel)
}

func (broker *Broker) channel(ctx context.Context) (*amqp.Channel, error) {
	err := ctx.Err()
	if err != nil {
		return nil, fmt.Errorf("open rabbitmq channel: %w", err)
	}

	var lastErr error

	attemptsRemaining := rabbitMQChannelOpenAttempts
	for attemptsRemaining > 0 {
		attemptsRemaining--

		conn, err := broker.currentConnection()
		if err != nil {
			return nil, err
		}

		channel, err := openRabbitMQChannel(ctx, conn)
		if err == nil {
			return channel, nil
		}

		lastErr = err

		if ctx.Err() != nil {
			return nil, fmt.Errorf("open rabbitmq channel: %w", err)
		}

		err = broker.reconnect(ctx, conn)
		if err != nil {
			return nil, errors.Join(lastErr, err)
		}
	}

	return nil, fmt.Errorf("open rabbitmq channel after reconnect: %w", lastErr)
}

func (broker *Broker) currentConnection() (*amqp.Connection, error) {
	broker.mu.Lock()
	defer broker.mu.Unlock()

	if broker.connection == nil {
		return nil, errBrokerClosed
	}

	return broker.connection, nil
}

func openRabbitMQChannel(
	ctx context.Context,
	conn *amqp.Connection,
) (*amqp.Channel, error) {
	openCtx, cancel := context.WithTimeout(ctx, rabbitMQChannelOpenTimeout)
	defer cancel()

	resultc := make(chan channelOpenResult, 1)

	go func() {
		channel, err := conn.Channel()
		result := channelOpenResult{channel: channel, err: err}

		select {
		case resultc <- result:
		case <-openCtx.Done():
			if channel != nil {
				_ = channel.Close()
			}
		}
	}()

	select {
	case result := <-resultc:
		if result.err != nil {
			return nil, fmt.Errorf("open rabbitmq channel: %w", result.err)
		}

		return result.channel, nil
	case <-openCtx.Done():
		return nil, fmt.Errorf("open rabbitmq channel: %w", openCtx.Err())
	}
}

func closeRabbitMQChannelAsync(channel *amqp.Channel) {
	go func() {
		_ = channel.Close()
	}()
}

func (broker *Broker) reconnect(
	ctx context.Context,
	stale *amqp.Connection,
) error {
	err := ctx.Err()
	if err != nil {
		return fmt.Errorf("reconnect rabbitmq: %w", err)
	}

	conn, err := dialRabbitMQ(broker.config)
	if err != nil {
		return err
	}

	broker.mu.Lock()
	defer broker.mu.Unlock()

	if broker.connection != stale {
		_ = conn.Close()

		return nil
	}

	broker.connection = conn

	go func() {
		_ = stale.Close()
	}()

	return nil
}

func clampPriority(priority int) uint8 {
	if priority < 0 {
		return 0
	}

	if priority > 100 {
		return 100
	}

	return uint8(priority)
}

var _ scheduler.Broker = (*Broker)(nil)
