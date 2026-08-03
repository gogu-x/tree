// Package actor provides a pragmatic Actor model for building concurrent
package tree

import (
	"fmt"
	"sync/atomic"
)

// Actor is the interface all actors must implement.
type Actor interface {
	Name() string
	// OnInit is called when the actor starts, before any messages are processed.
	OnInit(ctx Context)
	// HandleMessage processes a single message sent to this actor.
	HandleMessage(ctx Context, message interface{})
	// OnStop is called when the actor is shutting down.
	OnStop(ctx Context)
}

// MailboxSizer is an optional interface an Actor may implement to override
// the default user-message buffer size of its mailbox.
type MailboxSizer interface {
	MailboxSize() int
}

// LoggerProvider is an optional interface an Actor may implement to override
// the logger used to report panics raised by its own callbacks.
type LoggerProvider interface {
	Logger() Logger
}

// PID is the unique address of an actor. It is a value type and can be
// safely copied across goroutine boundaries.
type PID struct {
	ID   uint64 // auto-assigned unique identifier
	Name string // human-readable name
}

var nextPID uint64

func allocatePID(name string) PID {
	return PID{
		ID:   atomic.AddUint64(&nextPID, 1),
		Name: name,
	}
}

func (p PID) String() string {
	return fmt.Sprintf("%s#%d", p.Name, p.ID)
}

// systemMessage types are used for internal lifecycle management.
type systemMessage int

const (
	systemStop systemMessage = iota
)
