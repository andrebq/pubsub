package pubsub

import (
	"context"
	"errors"
	"runtime"
)

type (
	// D represents the 'D'ata that can be sent over a PubSub channel.
	D interface {
		any
	}

	// Broker implements the main pubsub logic
	Broker[Datatype D] struct {
		incomingData chan Datatype
		done         chan empty
		stop         chan empty

		closeErr error

		subscribe   chan subscription[Datatype]
		unsubscribe chan subscription[Datatype]
	}

	// helper types to make the code a bit more readable
	subscription[Datatype D] chan Datatype
	subscribers[Datatype D]  map[subscription[Datatype]]empty
	empty        struct{}
)

var (
	errBrokerIsClosed        = errors.New("pubsub: broker closed")
	errSubscriptionCancelled = errors.New("pubsub: subscription cancelled")
)

// NewBroker returns a new Broker that can buffer the given ammount of items
// before blocking a producer.
//
// If buffer == 0, then broker will block until it finishes the notification
// of all subscriptions.
//
// If buffer < 0, buffer = runtime.NumCPU()
func NewBroker[Datatype D](buffer int) *Broker[Datatype] {
	if buffer < 0 {
		buffer = runtime.NumCPU()
	}
	return &Broker[Datatype]{
		incomingData: make(chan Datatype, buffer),
		done:         make(chan empty),
		stop:         make(chan empty),
		subscribe:    make(chan subscription[Datatype], runtime.NumCPU()),
		unsubscribe:  make(chan subscription[Datatype], runtime.NumCPU()),
	}
}

// NewRunningBroker returns a broker which is running on a background goroutine,
// it is equivalent to `go NewBroker(buffer).Run(ctx)`
func NewRunningBroker[Datatype D](ctx context.Context, buffer int) *Broker[Datatype] {
	b := NewBroker[Datatype](buffer)
	go b.Run(ctx)
	return b
}

// Run the broker until ctx is closed
func (b *Broker[Datatype]) Run(ctx context.Context) error {
	subs := make(subscribers[Datatype])
	b.closeErr = errBrokerIsClosed
	defer func() {
		close(b.done)
	}()
	for {
		select {
		case <-ctx.Done():
			b.closeErr = ctx.Err()
			return b.closeErr
		case data := <-b.incomingData:
			subs.publish(ctx, data)
		case sub := <-b.subscribe:
			subs.put(sub)
		case sub := <-b.unsubscribe:
			subs.del(sub)
		}
	}
}

// WaitForData from one of the producers or until the context is done
// or Close() error ins called in the Broker
func (b *Broker[Datatype]) WaitForData(ctx context.Context, out *Datatype) error {
	sub, err := b.doSubscribe(ctx)
	if err != nil {
		return err
	}
	defer b.doUnsubscribe(sub)
	select {
	case d, subscribed := <-sub:
		if !subscribed {
			return errSubscriptionCancelled
		}
		*out = d
		return nil
	case <-b.done:
		return b.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Publish data to the broker (into the incoming buffer) or returns if the context
// was closed.
//
// Publish returns as soon as the broker receives the data but before the data is delivered
// to all consumers. There is no guarantee that data will eventually reach consumers.
func (b *Broker[Datatype]) Publish(ctx context.Context, data Datatype) error {
	select {
	case <-b.done:
		return b.closeErr
	case <-ctx.Done():
		return ctx.Err()
	case b.incomingData <- data:
		return nil
	}
}

// Close the broker and waits until the broker has completed its shutdown process
func (b *Broker[Datatype]) Close() error {
	select {
	case <-b.done:
		return b.closeErr
	case b.stop <- empty{}:
	}
	<-b.done
	return nil
}

func (b *Broker[Datatype]) doUnsubscribe(sub subscription[Datatype]) {
	select {
	case <-b.done:
	case b.unsubscribe <- sub:
	}
}

func (b *Broker[Datatype]) doSubscribe(ctx context.Context) (subscription[Datatype], error) {
	sub := make(subscription[Datatype], 1)
	select {
	case <-b.done:
		return nil, b.closeErr
	case b.subscribe <- sub:
	}
	return sub, nil
}

func (s subscribers[Datatype]) put(sub subscription[Datatype]) {
	_, has := s[sub]
	if !has {
		s[sub] = empty{}
	}
}

func (s subscribers[Datatype]) del(sub subscription[Datatype]) {
	_, has := s[sub]
	if has {
		delete(s, sub)
		close(sub)
	}
}

func (s subscribers[Datatype]) clear() {
	for sub := range s {
		close(sub)
	}
}

func (s subscribers[Datatype]) publish(ctx context.Context, value Datatype) {
	// TODO: think of better implementations, but for a proof-of-concept this is good enough
	//
	// Here, any subscriber that does not have capacity to receive the message will miss the
	// entry. Callers should include a buffer big enough to reduce the probability of lost messages
	for sub := range s {
		select {
		case sub <- value:
		case <-ctx.Done():
			// TODO: should we have some special treatment here?
		default:
		}
	}
}
