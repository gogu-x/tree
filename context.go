package actor

// ActorContext provides the message processing context for an Actor.
type ActorContext interface {
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
	// Request delivers a message to the target actor and returns a Future.
	Request(pid PID, msg interface{}) *Future
	// Response sends a reply for the current request. If the current message
	// was not sent via Request, this is a no-op.
	Response(value interface{}, err error)
	// Future returns the pending Future for the current request message.
	// Returns nil if the current message was not sent via Request.
	Future() *Future
	// Stop signals the current actor to shut down after processing the
	// current message.
	Stop()
	// Lookup returns the PID registered under the given name.
	// Returns a zero PID and false if not found.
	Lookup(name string) (PID, bool)
	// Register registers the current actor under an additional name.
	// Useful when the actor's addressable name is known only after initialization (e.g. after login).
	Register(name string)
	// System returns the ActorSystem this actor belongs to.
	System() *ActorSystem
}

// localContext implements ActorContext for actors managed by an ActorSystem.
type localContext struct {
	self   PID
	system *ActorSystem
	sender PID
	msg    interface{}
	future *Future
}

func (c *localContext) Self() PID               { return c.self }
func (c *localContext) Sender() PID             { return c.sender }
func (c *localContext) Message() interface{}    { return c.msg }
func (c *localContext) Future() *Future         { return c.future }
func (c *localContext) System() *ActorSystem    { return c.system }
func (c *localContext) Send(pid PID, msg interface{}) bool {
	return c.system.send(pid, msg, c.self)
}
func (c *localContext) TrySend(pid PID, msg interface{}) bool {
	return c.system.trySend(pid, msg, c.self)
}
func (c *localContext) Response(value interface{}, err error) {
	if c.future != nil {
		c.future.Respond(value, err)
	}
}
func (c *localContext) Stop() { c.system.stop(c.self) }
func (c *localContext) Request(pid PID, msg interface{}) *Future {
	return c.system.request(pid, msg, c.self)
}
func (c *localContext) Lookup(name string) (PID, bool) {
	return c.system.Lookup(name)
}
func (c *localContext) Register(name string) {
	c.system.Register(name, c.self)
}
