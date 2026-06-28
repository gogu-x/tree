package actor

// SpawnOption configures an actor process at spawn time.
type SpawnOption func(*spawnConfig)

// Logger is the minimal interface required for logging in the actor system.
// If not provided, a default logger writing to stderr is used.
type Logger interface {
	Error(format string, a ...interface{})
}

type spawnConfig struct {
	mailboxSize int
	logger      Logger
}

func defaultSpawnConfig() spawnConfig {
	return spawnConfig{
		mailboxSize: 128,
		logger:      defaultLogger,
	}
}

// WithMailboxSize sets the user message channel buffer size for the actor.
func WithMailboxSize(size int) SpawnOption {
	return func(c *spawnConfig) {
		if size > 0 {
			c.mailboxSize = size
		}
	}
}

// WithLogger sets the logger for the spawned actor's process.
func WithLogger(l Logger) SpawnOption {
	return func(c *spawnConfig) {
		if l != nil {
			c.logger = l
		}
	}
}
