package tlog

var Log *Logger

func NewLog(pathname string, flag int) {
	Loggers, err := New("debug", pathname, flag)
	if err != nil {
		return
	}
	Log = Loggers
}
