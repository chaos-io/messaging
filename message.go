package messaging

type Message struct {
	Id         string
	TraceId    string
	SpanId     string
	Attributes map[string]any
	Data       string
}
