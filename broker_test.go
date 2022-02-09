package pubsub

import (
	"context"
	"testing"
	"time"
)

func testIntPublish(t *testing.T, expectedValue int) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	broker := NewRunningBroker(ctx, 1)
	sub1, sub2 := make(chan D), make(chan D)

	waitForData := func(out chan D) {
		defer close(out)
		d, err := broker.WaitForData(ctx)
		if err != nil {
			t.Error("unable to receive data", err)
			return
		}
		if actualValue, ok := d.(int); !ok {
			t.Error("Invalid type")
		} else if actualValue != expectedValue {
			t.Errorf("Subscription %p got %v instead of %v", out, d, expectedValue)
		}
		out <- d
	}

	go waitForData(sub1)
	go waitForData(sub2)

	// wait for a few moments, otherwise one of the subscribers
	// might subscribe after the message has been published
	go func() {
		// 1ms should be enough time for the previous 2 goroutines
		// to subscribe to the broker
		time.Sleep(time.Millisecond)
		if err := broker.Publish(ctx, expectedValue); err != nil {
			t.Error(err)
		}
	}()

	<-sub1
	<-sub2
}

func TestBroker(t *testing.T) {
	testIntPublish(t, 1)
}
