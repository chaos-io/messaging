package messaging

type SubMessage struct {
	Message

	didAck     bool
	ack        func()
	nak        func()
	term       func()
	inProgress func()
}

func (m *SubMessage) SetAck(ack func()) {
	if m != nil {
		m.ack = ack
	}
}

func (m *SubMessage) SetNak(nak func()) {
	if m != nil {
		m.nak = nak
	}
}

func (m *SubMessage) SetTerm(term func()) {
	if m != nil {
		m.term = term
	}
}

func (m *SubMessage) SetInProgress(inProgress func()) {
	if m != nil {
		m.inProgress = inProgress
	}
}

func (m *SubMessage) Ack() {
	if m != nil && !m.didAck && m.ack != nil {
		m.ack()
		m.didAck = true
	}
}

func (m *SubMessage) Nak() {
	if m != nil && !m.didAck && m.nak != nil {
		m.nak()
		m.didAck = true
	}
}

func (m *SubMessage) Term() {
	if m != nil && !m.didAck && m.term != nil {
		m.term()
		m.didAck = true
	}
}

func (m *SubMessage) InProgress() {
	if m != nil && m.inProgress != nil {
		m.inProgress()
	}
}
