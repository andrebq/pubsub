package pubsub

import (
	"context"
	"errors"
	"runtime"
)

type (
	// D represents the 'D'ata that can be sent over a PubSub channel.
	D interface{}

	// Broker implements the main pubsub logic
	Broker struct {
		incomingData chan D
		done         chan empty
		stop         chan empty

		closeErr error

		subscribe   chan subscription
		unsubscribe chan subscription
	}

	// helper types to make the code a bit more readable
	subscription chan D
	subscribers  map[subscription]empty
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
func NewBroker(buffer int) *Broker {
	if buffer < 0 {
		buffer = runtime.NumCPU()
	}
	return &Broker{
		incomingData: make(chan D, buffer),
		done:         make(chan empty),
		stop:         make(chan empty),
		subscribe:    make(chan subscription, runtime.NumCPU()),
		unsubscribe:  make(chan subscription, runtime.NumCPU()),
	}
}

// NewRunningBroker returns a broker which is running on a background goroutine,
// it is equivalent to `go NewBroker(buffer).Run(ctx)`
func NewRunningBroker(ctx context.Context, buffer int) *Broker {
	b := NewBroker(buffer)
	go b.Run(ctx)
	return b
}

// Run the broker until ctx is closed
func (b *Broker) Run(ctx context.Context) error {
	subs := make(subscribers)
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
func (b *Broker) WaitForData(ctx context.Context) (D, error) {
	sub, err := b.doSubscribe(ctx)
	if err != nil {
		return nil, err
	}
	defer b.doUnsubscribe(sub)
	select {
	case d, subscribed := <-sub:
		if !subscribed {
			return nil, errSubscriptionCancelled
		}
		return d, nil
	case <-b.done:
		return nil, b.closeErr
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Publish data to the broker (into the incoming buffer) or returns if the context
// was closed.
//
// Publish returns as soon as the broker receives the data but before the data is delivered
// to all consumers. There is no guarantee that data will eventually reach consumers.
func (b *Broker) Publish(ctx context.Context, data D) error {
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
func (b *Broker) Close() error {
	select {
	case <-b.done:
		return b.closeErr
	case b.stop <- empty{}:
	}
	<-b.done
	return nil
}

func (b *Broker) doUnsubscribe(sub subscription) {
	select {
	case <-b.done:
	case b.unsubscribe <- sub:
	}
}

func (b *Broker) doSubscribe(ctx context.Context) (subscription, error) {
	sub := make(subscription, 1)
	select {
	case <-b.done:
		return nil, b.closeErr
	case b.subscribe <- sub:
	}
	return sub, nil
}

func (s subscribers) put(sub subscription) {
	_, has := s[sub]
	if !has {
		s[sub] = empty{}
	}
}

func (s subscribers) del(sub subscription) {
	_, has := s[sub]
	if has {
		delete(s, sub)
		close(sub)
	}
}

func (s subscribers) clear() {
	for sub := range s {
		close(sub)
	}
}

func (s subscribers) publish(ctx context.Context, value D) {
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
