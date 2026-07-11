package tree

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// ErrTimeout is returned when a Future.AwaitTimeout expires.
var ErrTimeout = errors.New("actor: timeout")

// ErrActorNotFound is returned when a target PID is not registered.
var ErrActorNotFound = errors.New("actor: not found")

// Future represents a pending result from a Request sent to another actor.
type Future struct {
	resultCh chan FutureResult

	pipeOnce sync.Once
	pipeSys  *Tree
	pipePID  PID
	pipeTag  interface{}
	pipeCb   func(interface{}, error) // closure callback
	piped    int32                    // atomic flag
}

// FutureResult holds the value and error produced by the responding actor.
type FutureResult struct {
	Value interface{}
	Err   error
}

// PipeResult is the message delivered to an actor's mailbox when a piped
// Future resolves (used by Pipe/PipeWithTag).
type PipeResult struct {
	Value interface{}
	Err   error
	Tag   interface{}
}

// pipeCallback is an internal message that carries a closure to be executed
// in the target actor's goroutine. Not exported ?users never see this type.
type pipeCallback struct {
	cb    func(interface{}, error)
	value interface{}
	err   error
}

// NewFuture creates a new Future with a 1-capacity result channel.
func NewFuture() *Future {
	return &Future{resultCh: make(chan FutureResult, 1)}
}

// Respond delivers a value (and optional error) to the waiting caller.
func (f *Future) Respond(value interface{}, err error) {
	if atomic.LoadInt32(&f.piped) == 1 {
		if f.pipeCb != nil {
			f.pipeSys.sendRaw(f.pipePID, pipeCallback{cb: f.pipeCb, value: value, err: err})
		} else {
			f.pipeSys.Send(f.pipePID, PipeResult{Value: value, Err: err, Tag: f.pipeTag})
		}
		return
	}
	f.resultCh <- FutureResult{Value: value, Err: err}
}

// Await blocks until the result is available and returns it.
func (f *Future) Await() (interface{}, error) {
	r := <-f.resultCh
	return r.Value, r.Err
}

// AwaitTimeout blocks for at most the given duration.
func (f *Future) AwaitTimeout(d time.Duration) (interface{}, error) {
	select {
	case r := <-f.resultCh:
		return r.Value, r.Err
	case <-time.After(d):
		return nil, ErrTimeout
	}
}

// Pipe delivers the result as a PipeResult message to the calling actor.
func (f *Future) Pipe(ctx Context) {
	f.setupPipe(ctx, nil, nil)
}

// PipeWithTag is like Pipe but attaches a tag to distinguish responses.
func (f *Future) PipeWithTag(ctx Context, tag interface{}) {
	f.setupPipe(ctx, tag, nil)
}

func (f *Future) Callback(ctx Context, cb func(interface{}, error)) {
	f.setupPipe(ctx, nil, cb)
}

func (f *Future) setupPipe(ctx Context, tag interface{}, cb func(interface{}, error)) {
	f.pipeOnce.Do(func() {
		f.pipeSys = ctx.System()
		f.pipePID = ctx.Self()
		f.pipeTag = tag
		f.pipeCb = cb
		atomic.StoreInt32(&f.piped, 1)

		// If Respond was called before setup, drain and forward.
		select {
		case r := <-f.resultCh:
			if f.pipeCb != nil {
				f.pipeSys.sendRaw(f.pipePID, pipeCallback{cb: f.pipeCb, value: r.Value, err: r.Err})
			} else {
				f.pipeSys.Send(f.pipePID, PipeResult{Value: r.Value, Err: r.Err, Tag: f.pipeTag})
			}
		default:
		}
	})
}
