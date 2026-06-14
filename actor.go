// Package actor provides a pragmatic Actor model for building concurrent
// message-passing systems. It is designed as a lightweight alternative to
// the chanrpc + skeleton pattern used elsewhere in Leaf.
package actor

import (
	"fmt"
	"sync/atomic"
)

// Actor is the interface all actors must implement.
type Actor interface {
	// OnInit is called when the actor starts, before any messages are processed.
	OnInit(ctx ActorContext)
	// HandleMessage processes a single message sent to this actor.
	HandleMessage(ctx ActorContext, message interface{})
	// OnStop is called when the actor is shutting down.
	OnStop(ctx ActorContext)
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

// messageEnvelope wraps a user message with sender information.
type messageEnvelope struct {
	msg    interface{}
	sender PID
	values map[string]interface{}
}

// requestEnvelope wraps a user message with sender information and a Future
// for request/response patterns.
type requestEnvelope struct {
	msg    interface{}
	sender PID
	future *Future
	values map[string]interface{}
}

// timerCallback is delivered to an actor's mailbox when a timer fires,
// so the callback executes in the same goroutine as HandleMessage.
type timerCallback struct {
	cb func(ActorContext)
}
