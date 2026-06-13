package main

import (
	"sync"
	"testing"
	"time"
)

// TestRejectResidual pins finding #20: the resting-cap drop must never produce
// an ack that contradicts the fills the caller emits. With zero fills the whole
// order is rejected (book_full); with fills the ack stays accepted and only the
// dropped residual is flagged (residual_dropped).
func TestRejectResidual(t *testing.T) {
	// No fills produced: the order itself is rejected.
	noFill := Ack{Type: "ack", Status: "accepted"}
	rejectResidual(&noFill, nil)
	if noFill.Status != "rejected" || noFill.Reason != "book_full" {
		t.Fatalf("zero fills: want rejected/book_full, got %s/%s", noFill.Status, noFill.Reason)
	}

	// Fills produced, residual could not rest: stay accepted, distinct reason —
	// a "rejected" ack alongside real fills for the same order is inconsistent.
	withFills := Ack{Type: "ack", Status: "accepted"}
	rejectResidual(&withFills, []FillDelivery{{Fill: Fill{Type: "fill", Qty: 1}}})
	if withFills.Status != "accepted" || withFills.Reason != "residual_dropped" {
		t.Fatalf("with fills: want accepted/residual_dropped, got %s/%s", withFills.Status, withFills.Reason)
	}
}

// TestClientEnqueueNonBlocking pins finding #2's core invariant: a stalled
// client's queue overflows by dropping-and-counting and NEVER blocks the
// caller (an output writer, hence the matcher fan-out behind it). We simulate a
// permanently stalled writer by priming the queue without starting writeLoop.
func TestClientEnqueueNonBlocking(t *testing.T) {
	c := &Client{}
	// Trip the once so enqueue uses our (un-drained) queue instead of starting
	// a real writeLoop that would need a live socket.
	c.outInit.Do(func() { c.outQ = make(chan any, clientOutQueue) })

	// Push far more than the queue can hold. Each call must return promptly; the
	// overflow must be counted, not block. A blocked enqueue would hang here and
	// the test would time out.
	const n = clientOutQueue * 3
	done := make(chan struct{})
	go func() {
		for i := 0; i < n; i++ {
			c.enqueue(Ack{Type: "ack"})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("enqueue blocked on a full queue instead of dropping")
	}

	if got := c.outDropped.Load(); got == 0 {
		t.Fatalf("expected overflow drops to be counted, got %d", got)
	}
	if want := uint64(n - clientOutQueue); c.outDropped.Load() != want {
		t.Fatalf("drop count = %d, want %d", c.outDropped.Load(), want)
	}
}

// TestStalledClientDoesNotStallOthers is the end-to-end shape of finding #2: a
// non-reading client on the disruptor's output stage must not stall acks to a
// healthy second client. We drive outputLoop directly: one client is "stalled"
// (queue primed, no writeLoop, so it drops) and one is "healthy" (a writeLoop
// substitute that records deliveries). outputLoop must deliver every message
// destined for the healthy client regardless of the stalled one.
func TestStalledClientDoesNotStallOthers(t *testing.T) {
	de := NewDisruptorEngine("normal")

	stalled := &Client{}
	stalled.outInit.Do(func() { stalled.outQ = make(chan any, clientOutQueue) })

	healthy := &Client{}
	var mu sync.Mutex
	var got int
	healthy.outInit.Do(func() {
		healthy.outQ = make(chan any, clientOutQueue)
		go func() {
			for range healthy.outQ {
				mu.Lock()
				got++
				mu.Unlock()
			}
		}()
	})

	go de.outputLoop()

	const perClient = clientOutQueue * 2 // enough to overflow the stalled queue
	for i := 0; i < perClient; i++ {
		de.out <- outMsg{client: stalled, payload: Ack{Type: "ack"}}
		de.out <- outMsg{client: healthy, payload: Ack{Type: "ack"}}
	}

	deadline := time.After(5 * time.Second)
	for {
		mu.Lock()
		n := got
		mu.Unlock()
		if n == perClient {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("healthy client received only %d/%d acks; stalled client stalled the output stage", n, perClient)
		default:
			time.Sleep(time.Millisecond)
		}
	}

	close(de.out)
	close(healthy.outQ)
}
