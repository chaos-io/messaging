//go:build local
// +build local

package nats

import (
	"context"
	"sync"
	"testing"

	"github.com/google/uuid"
	jsoniter "github.com/json-iterator/go"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"

	"github.com/chaos-io/chaos/pkg/config"
	"github.com/chaos-io/chaos/pkg/logs"
	"github.com/chaos-io/messaging"
)

func Test_PublishStartTask(t *testing.T) {
	traceId := uuid.New().String()
	logs.Infow("publish start task", "traceId", traceId)
	err := PublishStartTask(context.Background(), &startTaskRequest{Name: "task-" + traceId, Ids: []string{"1", "2"}})
	assert.NoError(t, err)
}

func Test_PublishStopTask(t *testing.T) {
	traceId := uuid.New().String()
	logs.Infow("publish stop task", "traceId", traceId)
	err := PublishStopTask(context.Background(), &stopTaskRequest{Name: "task-" + traceId})
	assert.NoError(t, err)
}

func Test_Subscription(t *testing.T) {
	_client, err := messaging.NewClient()
	if err != nil {
		logs.Fatalw("failed to create the message client", "error", err)
	}

	for _, sub := range _client.GetConfig().GetSubscriptions() {
		ep := sub.Endpoint
		if len(ep.Service) > 0 && ep.Service != "Agent" {
			continue
		}
		logs.Infof("AgentServer subscribed the %s topic", sub.Topic)
		_client.Subscribe(sub, func(ctx context.Context, s *messaging.Subscription, m *messaging.SubMessage) error {
			ctx = context.WithValue(ctx, messaging.TopicKey, s.Topic)
			ctx = context.WithValue(ctx, messaging.MessageKey, m)
			ctx = context.WithValue(ctx, messaging.MessageIdKey, m.Id)
			ctx = context.WithValue(ctx, messaging.MessageAttributesKey, m.Attributes)
			switch ep.Method {
			case "start_task":
				request := &startTaskRequest{}
				if err = jsoniter.ConfigFastest.UnmarshalFromString(m.Data, request); err != nil {
					logs.Warnw("failed to unmarshal the queue's message to json", "error", err)
					m.Term()
					return err
				}
				logs.Infow("received the message", "topic", s.Topic, "id", m.Id, "request", request)
				if err = startTask(ctx, request); err != nil {
					m.Nak()
				} else {
					m.Ack()
				}
			case "stop_tasks":
				request := &stopTaskRequest{}
				if err = jsoniter.ConfigFastest.UnmarshalFromString(m.Data, request); err != nil {
					logs.Warnw("failed to unmarshal the queue's message to json", "error", err)
					m.Term()
					return err
				}
				logs.Infow("received the message", "topic", s.Topic, "id", m.Id)
				if err = stopTask(ctx, request); err != nil {
					m.Nak()
				} else {
					m.Ack()
				}
			}
			return nil
		})
	}

	select {}
}

type startTaskRequest struct {
	Name string
	Ids  []string
}

func startTask(ctx context.Context, request *startTaskRequest) error {
	logs.Infow("starting task", "name", request.Name)
	return nil
}

type stopTaskRequest struct {
	Name string
}

func stopTask(ctx context.Context, request *stopTaskRequest) error {
	logs.Infow("stopping task", "name", request.Name)
	return nil
}

const (
	StartTaskTopic = "demo.start-task"
	StopTasksTopic = "demo.stop-tasks"
)

var (
	client     *messaging.Client
	clientOnce sync.Once
)

func GetMessaging() *messaging.Client {
	clientOnce.Do(func() {
		var err error
		client, err = messaging.NewClient()
		if err != nil {
			logs.Errorw("failed to create the message client", "error", err)
			panic(err)
		}
	})
	return client
}

func PublishStartTask(ctx context.Context, tsk *startTaskRequest) error {
	data, err := jsoniter.ConfigFastest.MarshalToString(tsk)
	if err != nil {
		return err
	}

	if err := GetMessaging().Publish(ctx, StartTaskTopic, &messaging.Message{Id: "1", Data: data}); err != nil {
		return err
	}

	logs.Debugw("published the start task message", "task", tsk)
	return nil
}

func PublishStopTask(ctx context.Context, tsk *stopTaskRequest) error {
	data, err := jsoniter.ConfigFastest.MarshalToString(tsk)
	if err != nil {
		return err
	}

	if err := GetMessaging().Publish(ctx, StopTasksTopic, &messaging.Message{Id: "2", Data: data}); err != nil {
		return err
	}

	logs.Debugw("published the stop task message", "task", tsk)
	return nil
}

func Test_ClearStream(t *testing.T) {
	cfg := &messaging.Config{}
	if err := config.ScanFrom(cfg, "messaging"); err != nil {
		logs.Debugw("failed to decode config", "error", err)
		return
	}

	nc, err := nats.Connect(cfg.ServiceName)
	if err != nil {
		logs.Errorw("failed to connect nats", "error", err)
		return
	}

	js, err := nc.JetStream()
	if err != nil {
		logs.Errorw("failed to connect nats", "error", err)
		return
	}

	streamNames := js.StreamNames()
	for name := range streamNames {
		logs.Infow("stream", "name", name)
		// logs.Infow("removing stream", "name", name)
		// err := js.DeleteStream(name)
		// if err != nil {
		// 	logs.Errorw("failed to remove stream", "name", name, "error", err)
		// }
	}
}
