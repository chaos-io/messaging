package nats

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/nats-io/nats.go"

	"github.com/chaos-io/chaos/pkg/logs"
	"github.com/chaos-io/messaging"
)

const defaultPullMaxWaiting = 128

func init() {
	messaging.Register("nats", func(cfg *messaging.Config) (messaging.Queue, error) {
		return New(cfg)
	})
}

type Nats struct {
	id            string
	conn          *nats.Conn
	js            nats.JetStreamContext
	subscriptions map[string]*nats.Subscription
	shutdown      bool
	shutdownCh    chan struct{}

	config     *messaging.Config
	pullNumber int32
}

func New(cfg *messaging.Config) (*Nats, error) {
	nc, err := nats.Connect(cfg.ServiceName)
	if err != nil {
		logs.Errorw("failed to connect nats", "error", err)
		return nil, err
	}

	n := &Nats{
		id:            "",
		conn:          nc,
		config:        cfg,
		subscriptions: map[string]*nats.Subscription{},
	}

	if cfg.Nats != nil && len(cfg.Nats.JetStream) > 0 {
		js, err := nc.JetStream()
		if err != nil {
			return nil, err
		}

		n.js = js

		if len(n.SubjectNames()) == 0 {
			n.SetSubjectNames(n.StreamName() + ".*")
		}

		n.createStream(n.StreamName(), n.SubjectNames())

		n.shutdownCh = make(chan struct{})
	}

	return n, nil
}

func (n *Nats) createStream(name string, subjects []string) {
	streamInfo, err := n.js.StreamInfo(name)
	if err != nil {
		logs.Warnw("failed to get the stream info", "name", name, "error", err)
	}

	if streamInfo == nil {
		streamCfg := n.NewStreamConfig(name, subjects)
		if _, err := n.js.AddStream(streamCfg); err != nil {
			logs.Warnw("failed to create stream", "name", name, "error", err)
		} else {
			logs.Infof("creating stream %q and subject %q", name, subjects)
		}
	} else {
		if len(subjects) > 0 {
			oldSubjects := streamInfo.Config.Subjects
			var nonExists []string
			if len(oldSubjects) == 0 {
				nonExists = subjects
			} else {
				set := make(map[string]struct{})
				for _, subject := range oldSubjects {
					set[subject] = struct{}{}
				}

				for _, subject := range subjects {
					if _, exist := set[subject]; !exist {
						nonExists = append(nonExists, subject)
					}
				}
			}

			allSubjects := append(nonExists, subjects...)
			streamConfig := n.NewStreamConfig(name, allSubjects)
			if _, err := n.js.UpdateStream(streamConfig); err != nil {
				logs.Warnw("failed to update stream", "name", name, "error", err)
			}
		}
	}
}

func (n *Nats) Publish(ctx context.Context, topic string, messages ...*messaging.Message) error {
	for _, message := range messages {
		bytes, err := jsoniter.Marshal(message)
		if err != nil {
			logs.Errorw("marshal message error", "message", message, "error", err)
			return err
		}

		if err = n.publish(ctx, topic, bytes); err != nil {
			return logs.NewErrorw("publish message error", "topic", topic, "error", err)
		}
	}

	logs.Infow("Nats: Published to queue", "topic", topic, "id", n.id)
	return nil
}

func (n *Nats) publish(ctx context.Context, topic string, msg []byte) error {
	_ = ctx

	if n.js != nil {
		n.updateSubjectName(topic)
		if _, err := n.js.Publish(topic, msg); err != nil {
			return err
		}
	} else {
		if err := n.conn.Publish(topic, msg); err != nil {
			return err
		}
	}

	return nil
}

func (n *Nats) updateSubjectName(name string) {
	if len(n.StreamName()) > 0 && len(name) > 0 {
		for _, s := range n.SubjectNames() {
			if s == name || s == n.StreamName()+".*" {
				return
			}
		}

		n.AppendSubjectName(name)
		streamCfg := n.NewStreamConfig(n.StreamName(), n.SubjectNames())
		if _, err := n.js.UpdateStream(streamCfg); err != nil {
			logs.Warnw("failed to update stream", "name", n.StreamName(), "subject", name, "error", err.Error())
			n.SetSubjectNames(n.SubjectNames()[0 : len(n.SubjectNames())-1]...)
		}
	}
}

func (n *Nats) Subscribe(s *messaging.Subscription, h messaging.Handler) {
	callback := func(m *nats.Msg) {
		msg := &messaging.SubMessage{}
		if err := jsoniter.Unmarshal(m.Data, msg); err != nil {
			logs.Warnw("Nats: failed to unmarshal data form topic", "topic", s.Topic, "error", err)
			return
		}

		msg.SetAck(func() {
			if err := m.Ack(); err != nil {
				logs.Warnw("Nats: failed to ack", "topic", s.Topic, "error", err)
				return
			}
		})
		msg.SetNak(func() {
			if err := m.Nak(); err != nil {
				logs.Warnw("Nats: failed to nak", "topic", s.Topic, "error", err)
				return
			}
		})
		msg.SetTerm(func() {
			if err := m.Term(); err != nil {
				logs.Warnw("Nats: failed to term", "topic", s.Topic, "error", err)
				return
			}
		})
		msg.SetInProgress(func() {
			if err := m.InProgress(); err != nil {
				logs.Warnw("Nats: failed to inProgress", "topic", s.Topic, "error", err)
				return
			}
		})

		if err := h(context.Background(), s, msg); err != nil {
			return
		}

		if s.AutoAck {
			msg.Ack()
		}
	}

	if len(s.Group) > 0 {
		sub, err := n.queueSubscribe(s, callback)
		n.subscriptions[s.Topic] = sub
		if err != nil {
			logs.Warnw("Nats: failed to subscribe with group", "topic", s.Topic, "group", s.Group, "error", err)
		}
	} else {
		sub, err := n.subscribe(s, callback)
		n.subscriptions[s.Topic] = sub
		if err != nil {
			logs.Warnw("Nats: failed to subscribe", "topic", s.Topic, "error", err)
		}
	}
}

func (n *Nats) queueSubscribe(s *messaging.Subscription, cb nats.MsgHandler) (sub *nats.Subscription, err error) {
	if n.js != nil {
		n.updateSubjectName(s.Topic)
		if s.Pull {
			sub, err = n.pullSubscribe(s, cb)
		} else {
			sub, err = n.js.QueueSubscribe(s.Topic, s.Group, cb)
		}
	} else {
		sub, err = n.conn.QueueSubscribe(s.Topic, s.Group, cb)
	}
	if err != nil {
		return nil, err
	}

	err = setPendingLimit(sub, s)
	return
}

func (n *Nats) subscribe(s *messaging.Subscription, cb nats.MsgHandler) (sub *nats.Subscription, err error) {
	if n.js != nil {
		n.updateSubjectName(s.Topic)
		sub, err = n.js.Subscribe(s.Topic, cb)
	} else {
		sub, err = n.conn.Subscribe(s.Topic, cb)
	}
	if err != nil {
		return nil, err
	}

	err = setPendingLimit(sub, s)
	return
}

var durableNameRegex = regexp.MustCompile("[a-zA-Z0-9_-]+")

func (n *Nats) pullSubscribe(s *messaging.Subscription, cb nats.MsgHandler) (*nats.Subscription, error) {
	// Create Pull based consumer with maximum 128 inflight.
	// PullMaxWaiting defines the max inflight pull requests.
	durableName := s.Group
	n.pullNumber++

	suffix := ""
	segments := strings.Split(s.Topic, ".")
	if len(segments) > 0 {
		suffix = segments[len(segments)-1]
	}
	if !durableNameRegex.MatchString(suffix) {
		suffix = strconv.Itoa(int(n.pullNumber))
	}

	durableName = strings.Join([]string{durableName, suffix}, "-")

	maxWaiting := s.PullMaxWaiting
	if maxWaiting == 0 {
		maxWaiting = defaultPullMaxWaiting
	}

	subOpts := []nats.SubOpt{nats.PullMaxWaiting(int(maxWaiting))}
	if len(s.AckTimeout) > 0 {
		ackTimeout, err := time.ParseDuration(s.AckTimeout)
		if err != nil {
			logs.Warnw("Nats: failed to parse ack timeout", "ackTimeout", s.AckTimeout, "error", err)
			return nil, err
		}
		subOpts = append(subOpts, nats.AckWait(ackTimeout))
	}

	subs, err := n.js.PullSubscribe(s.Topic, durableName, subOpts...)
	if err != nil {
		logs.Warnw("failed to pull subscribe the topic", "topic", s.Topic, "error", err)
		return nil, err
	}

	go func(s *messaging.Subscription, durableName string) {
		logs.Infow("nats subscribed", "topic", s.Topic, "subscribe", durableName)

		for {
			select {
			case <-n.shutdownCh:
				logs.Infow("shutdown the nats client, will closed the pull subscribe", "subscribe", durableName)
				return
			default:
			}

			logs.Debugw("fetching the next message", "subscribe", durableName)

			ms, _ := subs.Fetch(1)
			for _, msg := range ms {
				logs.Debugw("fetched the message", "subscribe", durableName, "subject", msg.Subject)
				cb(msg)
			}
		}
	}(s, durableName)

	return subs, nil
}

func setPendingLimit(sub *nats.Subscription, opts *messaging.Subscription) error {
	msgLimit := int(opts.PendingMsgLimit)
	bytesLimit := int(opts.PendingBytesLimit)
	if msgLimit != 0 || bytesLimit != 0 {
		if msgLimit == 0 {
			msgLimit = nats.DefaultSubPendingMsgsLimit
		}
		if bytesLimit == 0 {
			bytesLimit = nats.DefaultSubPendingBytesLimit
		}
		if err := sub.SetPendingLimits(msgLimit, bytesLimit); err != nil {
			logs.Warnw("failed to set pending limits", "name", opts.Name, "topic", opts.Topic,
				"msgLimit", msgLimit, "bytesLimit", bytesLimit, "error", err)
			return err
		}
	}
	return nil
}

// Shutdown shuts down all subscribers
func (n *Nats) Shutdown() {
	n.conn.Close()
	close(n.shutdownCh)
	n.shutdown = true
}

func (n *Nats) GetConn() *nats.Conn {
	if n != nil {
		return n.conn
	}
	return nil
}

func (n *Nats) StreamName() string {
	if n != nil && n.config != nil {
		return n.config.Nats.JetStream
	}
	return ""
}

func (n *Nats) SubjectNames() []string {
	if n != nil && n.config != nil {
		return n.config.Nats.TopicNames
	}
	return nil
}

func (n *Nats) AppendSubjectName(name string) {
	if n != nil && n.config != nil {
		n.config.Nats.TopicNames = append(n.config.Nats.TopicNames, name)
	}
}

func (n *Nats) SetSubjectNames(names ...string) {
	if n != nil && n.config != nil {
		n.config.Nats.TopicNames = names
	}
}

func (n *Nats) NewStreamConfig(name string, subjects []string) *nats.StreamConfig {
	streamCfg := &nats.StreamConfig{
		Name:     name,
		Subjects: subjects,
	}

	if n.MaxMsgs() > 0 {
		streamCfg.MaxMsgs = n.MaxMsgs()
	}
	if n.MaxAge() > 0 {
		streamCfg.MaxAge = time.Duration(n.MaxAge()) * time.Second
	}

	return streamCfg
}

func (n *Nats) MaxMsgs() int64 {
	if n != nil && n.config != nil {
		return n.config.Nats.MaxMsgs
	}
	return 0
}

func (n *Nats) MaxAge() int64 {
	if n != nil && n.config != nil {
		return n.config.Nats.MaxAge
	}
	return 0
}

func (n *Nats) GetJetStream() nats.JetStreamContext {
	if n != nil {
		return n.js
	}
	return nil
}
