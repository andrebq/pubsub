package pubsub

import (
	"context"
	"testing"
	"time"
)

func testGenericPublish[Datatype D](t *testing.T, expectedValue Datatype) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	broker := NewRunningBroker[Datatype](ctx, 1)
	sub1, sub2 := make(chan Datatype), make(chan Datatype)

	waitForData := func(out chan Datatype) {
		defer close(out)
		var d Datatype
		err := broker.WaitForData(ctx, &d)
		if err != nil {
			t.Error("unable to receive data", err)
			return
		}
		if d != expectedValue {
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
	testGenericPublish(t, int(1))
	testGenericPublish(t, float64(1.0))
}
