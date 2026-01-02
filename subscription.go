package messaging

import (
	"net/url"
)

type Subscription struct {
	Name  string
	Topic string
	Group string

	Pull              bool
	AutoAck           bool
	AckTimeout        string
	PullMaxWaiting    int64
	PendingMsgLimit   int64
	PendingBytesLimit int64
	Endpoint          *PushEndpoint
}

type PushEndpoint struct {
	Service string
	Method  string
	Url     *url.URL
}
