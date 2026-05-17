package actor

import (
	"fmt"
	"reflect"
)

// Handler is a function that handles a specific message type.
// It is called by the Router when a matching message arrives.
type Handler func(ctx ActorContext, msg interface{})

type Router struct {
	handlers map[reflect.Type]Handler
	fallback Handler
}

// Register maps a message type to a handler. The prototype argument is used
// only to extract the type ?its value is irrelevant.
//
//	r.Register(&LoginRequest{}, handleLogin)   // matches *LoginRequest
//	r.Register("", handleString)               // matches string
func (r *Router) Register(prototype interface{}, h Handler) {
	if r.handlers == nil {
		r.handlers = make(map[reflect.Type]Handler)
	}
	t := reflect.TypeOf(prototype)
	if _, dup := r.handlers[t]; dup {
		panic(fmt.Sprintf("actor.Router: handler already registered for %v", t))
	}
	r.handlers[t] = h
}

// SetFallback sets a handler for messages that don't match any registered type.
// If not set, unmatched messages are silently dropped.
func (r *Router) SetFallback(h Handler) {
	r.fallback = h
}

// Route dispatches msg to the registered handler based on its concrete type.
// Returns true if a handler was found and called.
func (r *Router) Route(ctx ActorContext, msg interface{}) bool {
	if r.handlers != nil {
		if h, ok := r.handlers[reflect.TypeOf(msg)]; ok {
			h(ctx, msg)
			return true
		}
	}
	if r.fallback != nil {
		r.fallback(ctx, msg)
		return true
	}
	return false
}
