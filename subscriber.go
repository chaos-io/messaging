package messaging

import (
	"context"
)

type Handler func(ctx context.Context, subscription *Subscription, m *SubMessage) error

//go:generate mockgen -destination=mocks/subscriber.go -package=mocks . Subscriber
type Subscriber interface {
	Subscribe(subscription *Subscription, handler Handler)
}
