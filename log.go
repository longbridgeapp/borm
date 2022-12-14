package borm

import (
	"log"
	"os"
)

// Logger is implemented by any logging system that is used for standard logs.
type Logger interface {
	Errorf(string, ...interface{})
	Warningf(string, ...interface{})
	Infof(string, ...interface{})
	Debugf(string, ...interface{})
	Printf(f string, v ...interface{})
	GetLogLevel() loggingLevel
}

// Errorf logs an ERROR log message to the logger specified in opts or to the
// global logger if no logger is specified in opts.
func (opt *Options) Errorf(format string, v ...interface{}) {
	if opt.Logger == nil {
		return
	}
	opt.Logger.Errorf(format, v...)
}

// Infof logs an INFO message to the logger specified in opts.
func (opt *Options) Infof(format string, v ...interface{}) {
	if opt.Logger == nil {
		return
	}
	opt.Logger.Infof(format, v...)
}

// Warningf logs a WARNING message to the logger specified in opts.
func (opt *Options) Warningf(format string, v ...interface{}) {
	if opt.Logger == nil {
		return
	}
	opt.Logger.Warningf(format, v...)
}

// Debugf logs a DEBUG message to the logger specified in opts.
func (opt *Options) Debugf(format string, v ...interface{}) {
	if opt.Logger == nil {
		return
	}
	opt.Logger.Debugf(format, v...)
}

type loggingLevel int

const (
	DEBUG loggingLevel = iota
	INFO
	WARNING
	ERROR
)

type defaultLog struct {
	*log.Logger
	level loggingLevel
}

func defaultLogger(level loggingLevel) *defaultLog {
	return &defaultLog{Logger: log.New(os.Stderr, "", log.LstdFlags), level: level}
}

func (l *defaultLog) Errorf(f string, v ...interface{}) {
	if l.level <= ERROR {
		l.Logger.Printf("ERROR: "+f, v...)
	}
}

func (l *defaultLog) Warningf(f string, v ...interface{}) {
	if l.level <= WARNING {
		l.Logger.Printf("WARNING: "+f, v...)
	}
}

func (l *defaultLog) Infof(f string, v ...interface{}) {
	if l.level <= INFO {
		l.Logger.Printf("INFO: "+f, v...)
	}
}

func (l *defaultLog) Debugf(f string, v ...interface{}) {
	if l.level <= DEBUG {
		l.Logger.Printf("DEBUG: "+f, v...)
	}
}

func (l *defaultLog) Printf(f string, v ...interface{}) {
	l.Logger.Printf("PRINT: "+f, v...)
}
func (l *defaultLog) GetLogLevel() loggingLevel {
	return l.level
}
