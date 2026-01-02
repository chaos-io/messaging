package messaging

import "context"

//go:generate mockgen -destination=mocks/publisher.go -package=mocks . Publisher
type Publisher interface {
	Publish(ctx context.Context, topic string, messages ...*Message) error
}
