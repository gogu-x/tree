package actor

import "sync"

// Mailbox implements a priority-combined channel for actor messages.
//
// System messages (lifecycle) are read before user messages (application),
// ensuring that Stop signals are not blocked behind a full user channel.
//
// Layout:
//
//	PushUser(msg) ?buffered chan ─?
//	                               ├→ merge goroutine ?Inbox() channel
//	PushSystem(msg)── unbuffered ──?    (system 优先)
type Mailbox struct {
	userChan   chan interface{} // buffered, for user messages
	systemChan chan interface{} // unbuffered, for system messages (high priority)
	inbox      chan interface{} // merged output, read by the actor goroutine
	closeCh    chan struct{}    // signals the merge goroutine to stop
	closeOnce  sync.Once
}

// NewMailbox creates a Mailbox with the given user channel buffer size.
func NewMailbox(userBufLen int) *Mailbox {
	mb := &Mailbox{
		userChan:   make(chan interface{}, userBufLen),
		systemChan: make(chan interface{}),
		inbox:      make(chan interface{}),
		closeCh:    make(chan struct{}),
	}
	go mb.merge()
	return mb
}

// merge runs in a dedicated goroutine and multiplexes user and system messages
// into the single inbox channel. System messages are prioritised: if both a
// system and a user message are ready, the system message is delivered first.
func (mb *Mailbox) merge() {
	defer close(mb.inbox)
	for {
		// Check for shutdown before anything else.
		select {
		case <-mb.closeCh:
			return
		default:
		}

		select {
		case <-mb.closeCh:
			return
		case msg := <-mb.systemChan:
			mb.inbox <- msg
		default:
			select {
			case <-mb.closeCh:
				return
			case msg := <-mb.systemChan:
				mb.inbox <- msg
			case msg := <-mb.userChan:
				mb.inbox <- msg
			}
		}
	}
}

// PushUser sends a user message into the buffered channel. It blocks only if
// the user channel is full (back-pressure).
func (mb *Mailbox) PushUser(msg interface{}) {
	mb.userChan <- msg
}

// TryPushUser attempts to send a user message without blocking. Returns false
// if the user channel is full.
func (mb *Mailbox) TryPushUser(msg interface{}) bool {
	select {
	case mb.userChan <- msg:
		return true
	default:
		return false
	}
}

// PushSystem sends a system message into the unbuffered channel. It blocks
// until the message is consumed by the merge goroutine.
func (mb *Mailbox) PushSystem(msg interface{}) {
	mb.systemChan <- msg
}

// Inbox returns a receive-only channel of merged messages.
func (mb *Mailbox) Inbox() <-chan interface{} {
	return mb.inbox
}

// Close signals the merge goroutine to shut down. Safe to call multiple times.
func (mb *Mailbox) Close() {
	mb.closeOnce.Do(func() {
		close(mb.closeCh)
	})
}
