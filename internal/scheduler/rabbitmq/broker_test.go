// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rabbitmq

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
