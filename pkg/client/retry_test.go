package client

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestWithRetriesSetsRetryCountAndInterval(t *testing.T) {
	c, err := NewClient("127.0.0.1", 623, "user", "pass")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	c.WithRetries(3, 20*time.Millisecond)
	if c.retryCount != 3 {
		t.Fatalf("retryCount = %d, want 3", c.retryCount)
	}
	if c.retryInterval != 20*time.Millisecond {
		t.Fatalf("retryInterval = %v, want 20ms", c.retryInterval)
	}

	// WithRetry keeps the upstream zero-interval API: it must not clear the
	// interval set by WithRetries, and vice versa.
	c.WithRetry(5)
	if c.retryCount != 5 {
		t.Fatalf("retryCount after WithRetry = %d, want 5", c.retryCount)
	}
	if c.retryInterval != 20*time.Millisecond {
		t.Fatalf("WithRetry must not change retryInterval, got %v", c.retryInterval)
	}
}

func TestWaitRetryIntervalNoopOnLastAttemptOrZeroInterval(t *testing.T) {
	c, err := NewClient("127.0.0.1", 623, "user", "pass")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	// Zero interval: upstream behavior, no sleep.
	if err := c.waitRetryInterval(context.Background(), 1, 3); err != nil {
		t.Fatalf("zero interval returned %v", err)
	}

	// Last attempt: there is no next try to wait for.
	c.retryInterval = time.Hour
	start := time.Now()
	if err := c.waitRetryInterval(context.Background(), 3, 3); err != nil {
		t.Fatalf("last attempt returned %v", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("last attempt slept for %v", elapsed)
	}
}

func TestWaitRetryIntervalSleepsConfiguredInterval(t *testing.T) {
	c, err := NewClient("127.0.0.1", 623, "user", "pass")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.retryInterval = 40 * time.Millisecond

	start := time.Now()
	if err := c.waitRetryInterval(context.Background(), 1, 3); err != nil {
		t.Fatalf("waitRetryInterval: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 35*time.Millisecond {
		t.Fatalf("waitRetryInterval returned after %v, want >= 35ms", elapsed)
	}
}

func TestWaitRetryIntervalPreservesCloseCause(t *testing.T) {
	c, err := NewClient("127.0.0.1", 623, "user", "pass")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.retryInterval = time.Hour

	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(net.ErrClosed)

	err = c.waitRetryInterval(ctx, 1, 3)
	if !errors.Is(err, net.ErrClosed) {
		t.Fatalf("err = %v, want net.ErrClosed in the chain", err)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled in the chain", err)
	}
}

func TestWaitRetryIntervalHonorsCancellation(t *testing.T) {
	c, err := NewClient("127.0.0.1", 623, "user", "pass")
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	c.retryInterval = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err = c.waitRetryInterval(ctx, 1, 3)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("cancelled wait took %v", elapsed)
	}
}
