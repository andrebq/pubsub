package pubsub

import (
	"context"
	"reflect"
	"runtime"
	"testing"
	"time"
)

func TestBroker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	broker := NewRunningBroker(ctx, 1)
	sub1, sub2 := make(chan D), make(chan D)

	sampleData := int(1)

	waitForData := func(out chan D) {
		defer close(out)
		d, err := broker.WaitForData(ctx)
		if err != nil {
			t.Error("unable to receive data", err)
			return
		}
		if !reflect.DeepEqual(d, sampleData) {
			t.Errorf("Subscription %p got %v instead of %v", out, d, sampleData)
		}
		out <- d
	}

	go waitForData(sub1)
	go waitForData(sub2)

	// wait for a few moments, otherwise one of the subscribers
	// might subscribe after the message has been published
	runtime.Gosched()
	broker.Publish(ctx, 1)

	<-sub1
	<-sub2
}
