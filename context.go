package tree

// Context provides the message processing context for an Actor.
type Context interface {
	// Self returns the PID of the current actor.
	Self() PID
	// Sender returns the PID of the actor that sent the current message.
	// Returns a zero PID if the message was sent without a sender.
	Sender() PID
	// Message returns the current message being processed.
	// Returns nil during OnInit and OnStop.
	Message() interface{}
	// Send delivers a message to the target actor asynchronously.
	// Returns false if the target PID is not registered.
	Send(pid PID, msg interface{}) bool
	// TrySend delivers a message without blocking. Returns false if the
	// target PID is not registered or the mailbox is full.
	TrySend(pid PID, msg interface{}) bool
	// Request delivers a message to the target actor and returns an Envelope
	// for synchronous waiting (Await/AwaitTimeout).
	Request(pid PID, msg interface{}) *Envelope
	// RequestCallback delivers a message to the target actor; cb is invoked
	// in this actor's goroutine (with this actor's Context) once the target
	// responds.
	RequestCallback(pid PID, msg interface{}, cb func(Context, interface{}, error)) *Envelope
	// RequestAsMessage delivers a message to the target actor; when the
	// target responds, the result value is redelivered to this actor's
	// mailbox as an ordinary message and processed by HandleMessage,
	// instead of via a dedicated callback or Await. Useful when all
	// responses should funnel through a single HandleMessage dispatch.
	// A non-nil response error is logged and not delivered.
	RequestAsMessage(pid PID, msg interface{}) *Envelope
	// Response sends a reply for the current request. If the current message
	// was not sent via Request/RequestCallback, this is a no-op.
	Response(value interface{}, err error)
	// RequestEnvelope returns the pending request Envelope for the current
	// message. Returns nil if the current message was not sent via Request.
	RequestEnvelope() *Envelope
	// Stop signals the current actor to shut down after processing the
	// current message.
	Stop()
	// Lookup returns the PID registered under the given name.
	// Returns a zero PID and false if not found.
	Lookup(name string) (PID, bool)
	// Register registers the current actor under an additional name.
	// Useful when the actor's addressable name is known only after initialization (e.g. after login).
	Register(name string)
	// System returns the Tree this actor belongs to.
	System() *Tree
	// SetValue stores a user-defined value associated with the given key.
	SetValue(key string, value interface{})
	// GetValue retrieves a user-defined value by key. Returns nil if not set.
	GetValue(key string) interface{}
}

// localContext implements Context for actors managed by an Tree.
type localContext struct {
	self    PID
	system  *Tree
	sender  PID
	msg     interface{}
	request *Envelope
	values  map[string]interface{}
}

func (c *localContext) Self() PID                  { return c.self }
func (c *localContext) Sender() PID                { return c.sender }
func (c *localContext) Message() interface{}       { return c.msg }
func (c *localContext) RequestEnvelope() *Envelope { return c.request }
func (c *localContext) System() *Tree              { return c.system }
func (c *localContext) Send(pid PID, msg interface{}) bool {
	return c.system.sendWithValues(pid, msg, c.self, c.values)
}
func (c *localContext) TrySend(pid PID, msg interface{}) bool {
	return c.system.trySendWithValues(pid, msg, c.self, c.values)
}
func (c *localContext) Response(value interface{}, err error) {
	if c.request != nil {
		c.request.Respond(value, err)
	}
}
func (c *localContext) Stop() { c.system.stop(c.self) }
func (c *localContext) Request(pid PID, msg interface{}) *Envelope {
	return c.system.requestWithValues(pid, msg, c.self, c.values, nil)
}
func (c *localContext) RequestCallback(pid PID, msg interface{}, cb func(Context, interface{}, error)) *Envelope {
	return c.system.requestWithValues(pid, msg, c.self, c.values, cb)
}
func (c *localContext) RequestAsMessage(pid PID, msg interface{}) *Envelope {
	return c.system.requestAsMessageWithValues(pid, msg, c.self, c.values)
}
func (c *localContext) Lookup(name string) (PID, bool) {
	return c.system.Lookup(name)
}
func (c *localContext) Register(name string) {
	c.system.Register(name, c.self)
}
func (c *localContext) SetValue(key string, value interface{}) {
	if c.values == nil {
		c.values = make(map[string]interface{})
	}
	c.values[key] = value
}
func (c *localContext) GetValue(key string) interface{} {
	if c.values == nil {
		return nil
	}
	return c.values[key]
}
