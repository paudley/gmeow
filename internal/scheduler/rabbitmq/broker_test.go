// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rabbitmq

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
	sched "blackcat.ca/gmeow/internal/scheduler"
)

func TestQueueNamesUseGmeowDotPrefix(t *testing.T) {
	topology := newTopology(testQueuePrefix)
	for _, name := range []string{
		topology.workQueue,
		topology.retryQueue,
		topology.failedQueue,
		topology.deadLetterQueue,
		topology.projectionQueue,
	} {
		if !strings.HasPrefix(name, "gmeow.test.") {
			t.Fatalf("queue %q does not use gmeow.test. prefix", name)
		}
	}
}

func TestRabbitMQDeliversHigherPriorityWorkFirst(t *testing.T) {
	cfg := testRabbitMQConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	broker, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	purgeQueues(t, broker)
	background := contracts.AnalyzerJob{
		SchemaVersion:  contracts.SchemaVersionPhase00,
		JobID:          "background-priority-test",
		IdempotencyKey: "background-priority-test",
		ObjectDigest:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Analyzer:       contracts.AnalyzerSpec{Name: "noop", Version: "1"},
		PriorityClass:  contracts.PriorityBackground,
		Priority:       10,
	}
	interactive := background
	interactive.JobID = "interactive-priority-test"
	interactive.IdempotencyKey = "interactive-priority-test"
	interactive.ObjectDigest = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	interactive.PriorityClass = contracts.PriorityInteractive
	interactive.Priority = 100
	if err := broker.Publish(ctx, background); err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(ctx, interactive); err != nil {
		t.Fatal(err)
	}

	channel, err := broker.connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer channel.Close()
	delivery, ok, err := channel.Get(broker.topology.workQueue, false)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected work delivery")
	}
	defer delivery.Nack(false, true)
	var delivered contracts.AnalyzerJob
	if err := json.Unmarshal(delivery.Body, &delivered); err != nil {
		t.Fatal(err)
	}
	if delivered.IdempotencyKey != interactive.IdempotencyKey {
		t.Fatalf("expected interactive work first, got %#v", delivered)
	}
}

func TestRabbitMQTopologyAndDeadLetterRequeue(t *testing.T) {
	cfg := testRabbitMQConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	broker, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	purgeQueues(t, broker)
	job := contracts.AnalyzerJob{
		SchemaVersion:  contracts.SchemaVersionPhase00,
		JobID:          "rabbitmq-test",
		IdempotencyKey: "rabbitmq-test",
		ObjectDigest:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Analyzer:       contracts.AnalyzerSpec{Name: "noop", Version: "1"},
		Priority:       10,
	}
	if err := broker.RouteFailure(ctx, job); err != nil {
		t.Fatal(err)
	}
	job.Attempt = 1
	if err := broker.RouteFailure(ctx, job); err != nil {
		t.Fatal(err)
	}
	dead, err := broker.DeadLetters(ctx, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) == 0 {
		t.Fatal("expected a dead-lettered job")
	}
	requeued, err := broker.RequeueDeadLetters(ctx, 5)
	if err != nil {
		t.Fatal(err)
	}
	if requeued == 0 {
		t.Fatal("expected dead-letter job to be requeued")
	}
}

func TestRabbitMQUnackedDeliveryRedeliversAfterChannelClose(t *testing.T) {
	cfg := testRabbitMQConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	broker, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	purgeQueues(t, broker)
	job := contracts.AnalyzerJob{
		SchemaVersion:  contracts.SchemaVersionPhase00,
		JobID:          "redelivery-test",
		IdempotencyKey: "redelivery-test",
		ObjectDigest:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Analyzer:       contracts.AnalyzerSpec{Name: "noop", Version: "1"},
		Priority:       10,
	}
	if err := broker.Publish(ctx, job); err != nil {
		t.Fatal(err)
	}
	channel, err := broker.connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := channel.Get(broker.topology.workQueue, false); err != nil || !ok {
		t.Fatalf("expected first delivery, ok=%t err=%v", ok, err)
	}
	if err := channel.Close(); err != nil {
		t.Fatal(err)
	}
	channel, err = broker.connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer channel.Close()
	delivery, ok, err := channel.Get(broker.topology.workQueue, false)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected unacked job to redeliver")
	}
	if err := delivery.Ack(false); err != nil {
		t.Fatal(err)
	}
}

func TestRabbitMQNackRetriesThenDeadLetters(t *testing.T) {
	cfg := testRabbitMQConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	broker, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	purgeQueues(t, broker)
	job := contracts.AnalyzerJob{
		SchemaVersion:  contracts.SchemaVersionPhase00,
		JobID:          "nack-test",
		IdempotencyKey: "nack-test",
		ObjectDigest:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Analyzer:       contracts.AnalyzerSpec{Name: "noop", Version: "1"},
		Priority:       10,
	}
	if err := broker.Publish(ctx, job); err != nil {
		t.Fatal(err)
	}
	nackOne(t, broker)
	if processed, err := broker.ProcessFailures(ctx, 10); err != nil || processed != 1 {
		t.Fatalf("expected one failed job processed, processed=%d err=%v", processed, err)
	}
	time.Sleep(cfg.RetryBackoff + 50*time.Millisecond)
	nackOne(t, broker)
	if processed, err := broker.ProcessFailures(ctx, 10); err != nil || processed != 1 {
		t.Fatalf(
			"expected exhausted failed job processed, processed=%d err=%v",
			processed,
			err,
		)
	}
	dead, err := broker.DeadLetters(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(dead) != 1 || dead[0].Attempt != 2 {
		t.Fatalf("expected exhausted job in dead letter, got %#v", dead)
	}
}

func TestFailedJobsPeeksWithoutConsuming(t *testing.T) {
	cfg := testRabbitMQConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	broker, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	purgeQueues(t, broker)
	job := contracts.AnalyzerJob{
		SchemaVersion:  contracts.SchemaVersionPhase00,
		JobID:          "failed-peek-test",
		IdempotencyKey: "failed-peek-test",
		ObjectDigest:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Analyzer:       contracts.AnalyzerSpec{Name: "noop", Version: "1"},
		Priority:       10,
	}
	if err := broker.Publish(ctx, job); err != nil {
		t.Fatal(err)
	}
	nackOne(t, broker)

	first, err := broker.FailedJobs(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	second, err := broker.FailedJobs(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("failed inspection consumed jobs, first=%#v second=%#v", first, second)
	}
}

func TestAnalysisJobSourcePersistsFailureCauseInFailedJob(t *testing.T) {
	cfg := testRabbitMQConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	broker, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	purgeQueues(t, broker)
	source := &AnalysisJobSource{connection: broker.connection, config: AnalysisJobSourceConfig{
		QueuePrefix: cfg.QueuePrefix,
	}}
	job := contracts.AnalyzerJob{
		SchemaVersion:  contracts.SchemaVersionPhase00,
		JobID:          "failed-cause-test",
		IdempotencyKey: "failed-cause-test",
		ObjectDigest:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Analyzer:       contracts.AnalyzerSpec{Name: "summary.model", Version: "1"},
	}

	if err := source.publishFailure(ctx, job, errors.New("model unavailable")); err != nil {
		t.Fatal(err)
	}
	failed, err := broker.FailedJobs(ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(failed) != 1 || failed[0].Failure != "model unavailable" {
		t.Fatalf("expected failure cause in failed job, got %#v", failed)
	}
}

func TestProcessProjectionRefreshesProjectsOncePerBatch(t *testing.T) {
	cfg := testRabbitMQConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	broker, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	purgeQueues(t, broker)
	for _, digest := range []contracts.ObjectDigest{
		"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	} {
		if err := broker.PublishProjectionRefresh(ctx, digest); err != nil {
			t.Fatal(err)
		}
	}
	projector := &countingProjector{}

	processed, err := broker.ProcessProjectionRefreshes(ctx, projector, 100)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 2 || projector.calls != 1 {
		t.Fatalf("expected one projection refresh batch, processed=%d calls=%d",
			processed,
			projector.calls,
		)
	}
	status, err := broker.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Pending != 0 {
		t.Fatalf("analysis queue should be unaffected by projection refresh, status=%#v", status)
	}
}

func TestRetryPublishingUsesExponentialBackoff(t *testing.T) {
	broker := &Broker{config: Config{RetryLimit: 3, RetryBackoff: 50 * time.Millisecond}}
	job := contracts.AnalyzerJob{
		IdempotencyKey: "retry-exp",
		Attempt:        1,
		Priority:       10,
	}

	first := broker.retryPublishing(job, []byte("{}"))
	if first.Expiration != "50" {
		t.Fatalf("expected first retry expiration 50ms, got %q", first.Expiration)
	}
	if first.Headers["retry_backoff_ms"] != int64(50) {
		t.Fatalf("expected first retry backoff header, got %#v", first.Headers)
	}

	job.Attempt = 2
	second := broker.retryPublishing(job, []byte("{}"))
	if second.Expiration != "100" {
		t.Fatalf("expected second retry expiration 100ms, got %q", second.Expiration)
	}
	if second.Headers["retry_backoff_ms"] != int64(100) {
		t.Fatalf("expected second retry backoff header, got %#v", second.Headers)
	}

	job.Attempt = 4
	deadLetter := broker.retryPublishing(job, []byte("{}"))
	if deadLetter.Expiration != "" {
		t.Fatalf("dead-letter publishing must not expire, got %q", deadLetter.Expiration)
	}
}

func TestRabbitMQScanIsIdempotent(t *testing.T) {
	cfg := testRabbitMQConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	broker, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	purgeQueues(t, broker)
	store := filestore.NewFilesystemStore(t.TempDir())
	if _, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader("hello"),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
	}); err != nil {
		t.Fatal(err)
	}
	service, err := sched.NewService(
		store,
		broker,
		[]contracts.AnalyzerSpec{{
			Name:       "text.extract",
			Version:    "1",
			MediaTypes: []string{"text/plain"},
		}},
		sched.Config{RetryBackoff: cfg.RetryBackoff},
	)
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.Scan(ctx, contracts.SchedulerScanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Scan(ctx, contracts.SchedulerScanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	status, err := broker.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.Enqueued != 1 || second.Enqueued != 0 || status.Pending != 1 {
		t.Fatalf(
			"expected one effective RabbitMQ job, first=%#v second=%#v status=%#v",
			first,
			second,
			status,
		)
	}
}

func TestReconcilePendingDropsSatisfiedAndDuplicateWork(t *testing.T) {
	cfg := testRabbitMQConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	broker, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	purgeQueues(t, broker)

	satisfied := testAnalyzerJob("satisfied", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	duplicate := testAnalyzerJob("duplicate", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	unique := testAnalyzerJob("unique", "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc")
	for _, job := range []contracts.AnalyzerJob{satisfied, duplicate, duplicate, unique} {
		if err := broker.Publish(ctx, job); err != nil {
			t.Fatal(err)
		}
	}

	response, err := broker.ReconcilePending(
		ctx,
		10,
		func(job contracts.AnalyzerJob) (bool, error) {
			return job.IdempotencyKey == satisfied.IdempotencyKey, nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if response.Checked != 4 ||
		response.DroppedSatisfied != 1 ||
		response.DroppedDuplicate != 1 ||
		response.Kept != 2 ||
		response.Republished != 2 {
		t.Fatalf("unexpected reconcile response: %#v", response)
	}

	pending, err := broker.PendingJobs(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]int{}
	for _, job := range pending {
		keys[job.IdempotencyKey]++
	}
	if len(pending) != 2 ||
		keys[duplicate.IdempotencyKey] != 1 ||
		keys[unique.IdempotencyKey] != 1 {
		t.Fatalf("unexpected reconciled pending jobs: %#v", pending)
	}
}

func TestReconcilePendingRecoversHeldJobsBeforeNextPass(t *testing.T) {
	cfg := testRabbitMQConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	broker, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer broker.Close()
	purgeQueues(t, broker)

	held := testAnalyzerJob("held", "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd")
	channel, err := broker.channel(ctx)
	if err != nil {
		t.Fatal(err)
	}
	body, err := json.Marshal(held)
	if err != nil {
		t.Fatal(err)
	}
	if err := channel.PublishWithContext(
		ctx,
		"",
		broker.topology.reconcileQueue,
		false,
		false,
		amqpPublishingForTest(held, body),
	); err != nil {
		t.Fatal(err)
	}
	if err := channel.Close(); err != nil {
		t.Fatal(err)
	}

	response, err := broker.ReconcilePending(
		ctx,
		10,
		func(contracts.AnalyzerJob) (bool, error) { return false, nil },
	)
	if err != nil {
		t.Fatal(err)
	}
	if response.Republished != 2 || response.Checked != 1 || response.Kept != 1 {
		t.Fatalf("expected held job recovered and reconciled once, got %#v", response)
	}

	pending, err := broker.PendingJobs(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].IdempotencyKey != held.IdempotencyKey {
		t.Fatalf("expected recovered held job in pending queue, got %#v", pending)
	}
}

func testAnalyzerJob(key string, digest contracts.ObjectDigest) contracts.AnalyzerJob {
	return contracts.AnalyzerJob{
		SchemaVersion:  contracts.SchemaVersionPhase00,
		JobID:          key,
		IdempotencyKey: key,
		ObjectDigest:   digest,
		Analyzer:       contracts.AnalyzerSpec{Name: "noop", Version: "1"},
		Priority:       10,
	}
}

func amqpPublishingForTest(job contracts.AnalyzerJob, body []byte) amqp.Publishing {
	return amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    job.IdempotencyKey,
		Priority:     uint8(clampPriority(job.Priority)),
		Body:         body,
	}
}

func testRabbitMQConfig(t *testing.T) Config {
	t.Helper()
	loaded, err := config.Load(config.Options{
		Path: filepath.Join("..", "..", "..", "gmeow.toml"),
	})
	if err != nil {
		t.Fatalf("load gmeow.toml for RabbitMQ integration tests: %v", err)
	}
	cfg := TestConfigFromResolved(loaded.Resolved.RabbitMQ, loaded.Resolved.Scheduler)
	cfg.RetryLimit = 1
	cfg.RetryBackoff = 50 * time.Millisecond
	return cfg
}

func nackOne(t *testing.T, broker *Broker) {
	t.Helper()
	channel, err := broker.connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer channel.Close()
	delivery, ok, err := channel.Get(broker.topology.workQueue, false)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected work delivery")
	}
	if err := delivery.Nack(false, false); err != nil {
		t.Fatal(err)
	}
}

func purgeQueues(t *testing.T, broker *Broker) {
	t.Helper()
	channel, err := broker.connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer channel.Close()
	for _, name := range []string{
		broker.topology.workQueue,
		broker.topology.reconcileQueue,
		broker.topology.retryQueue,
		broker.topology.failedQueue,
		broker.topology.deadLetterQueue,
		broker.topology.projectionQueue,
	} {
		if _, err := channel.QueuePurge(name, false); err != nil {
			t.Fatalf("purge %s: %v", name, err)
		}
	}
}

type countingProjector struct {
	calls int
}

func (projector *countingProjector) ProjectChanged(context.Context, time.Time) error {
	projector.calls++

	return nil
}
