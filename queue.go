package messaging

import "sync"

var (
	queues     map[string]Constructor
	queuesOnce sync.Once
)

//go:generate mockgen -destination=mocks/queue.go -package=mocks . Queue
type Queue interface {
	Publisher
	Subscriber

	Shutdown()
}

type Constructor func(config *Config) (Queue, error)

func Register(name string, constructor Constructor) {
	queuesOnce.Do(func() {
		queues = make(map[string]Constructor)
	})

	queues[name] = constructor
}
