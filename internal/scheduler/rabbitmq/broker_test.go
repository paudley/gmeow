// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rabbitmq

import (
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

func TestQueueNamesUseGmeowPrefix(t *testing.T) {
	for _, name := range []string{
		workQueueName,
		retryQueueName,
		deadLetterQueueName,
		projectionQueueName,
	} {
		if !strings.HasPrefix(name, "gmeow:") {
			t.Fatalf("queue %q does not use gmeow: prefix", name)
		}
	}
}

func TestRabbitMQTopologyAndDeadLetterRequeue(t *testing.T) {
	url := testRabbitMQURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	broker, err := New(ctx, Config{
		URL:          url,
		RetryLimit:   1,
		RetryBackoff: 50 * time.Millisecond,
		QueuePrefix:  "gmeow:",
	})
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
		Analyzer:       contracts.AnalyzerSpec{Name: "noop", Version: "1", Enabled: true},
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
	url := testRabbitMQURL(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	broker, err := New(ctx, Config{
		URL:         url,
		RetryLimit:  1,
		QueuePrefix: "gmeow:",
	})
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
		Analyzer:       contracts.AnalyzerSpec{Name: "noop", Version: "1", Enabled: true},
		Priority:       10,
	}
	if err := broker.Publish(ctx, job); err != nil {
		t.Fatal(err)
	}
	channel, err := broker.connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := channel.Get(workQueueName, false); err != nil || !ok {
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
	delivery, ok, err := channel.Get(workQueueName, false)
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

func testRabbitMQURL(t *testing.T) string {
	t.Helper()
	raw := os.Getenv("GMEOW_TEST_RABBITMQ_URL")
	if raw == "" {
		t.Skip("GMEOW_TEST_RABBITMQ_URL is required for RabbitMQ integration tests")
	}
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse GMEOW_TEST_RABBITMQ_URL: %v", err)
	}
	if parsed.Path == "" || parsed.Path == "/" {
		parsed.Path = "/gmeow-test"
		return parsed.String()
	}
	if parsed.Path != "/gmeow-test" {
		t.Fatalf(
			"GMEOW_TEST_RABBITMQ_URL must use RabbitMQ vhost /gmeow-test, got %q",
			parsed.Path,
		)
	}
	return parsed.String()
}

func purgeQueues(t *testing.T, broker *Broker) {
	t.Helper()
	channel, err := broker.connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer channel.Close()
	for _, name := range []string{workQueueName, retryQueueName, deadLetterQueueName, projectionQueueName} {
		if _, err := channel.QueuePurge(name, false); err != nil {
			t.Fatalf("purge %s: %v", name, err)
		}
	}
}
