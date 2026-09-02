package log_test

import (
	"github.com/gogu-x/tree/log"
)

func Example() {
	name := "Tree"

	log.Debug("name=%s", name)
	log.Release("service released: %s", name)
	log.Info("service started: %s", name)
	log.Warn("high load: %d%%", 90)
	log.Error("request failed: %s", "timeout")
	// log.Fatal("unrecoverable error")

	logger, err := log.New("release", "", log.DefaultFlags)
	if err != nil {
		return
	}
	defer logger.Close()

	logger.Debug("this record is filtered")
	logger.Release("release logger is ready")
}
