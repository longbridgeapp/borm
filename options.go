package borm

type Options struct {
	//default log level WARNING
	Logger Logger
	// default size 512MB
	MemTableSize int64
	// default false
	QueryAnalyzer bool
}

type Option func(*Options)

func newOptions(ops ...Option) *Options {
	opt := &Options{
		Logger:       defaultLogger(WARNING),
		MemTableSize: (64 << 20) * 8,
	}
	for _, o := range ops {
		o(opt)
	}
	return opt
}

func WithLoggingLevel(val loggingLevel) Option {
	return func(o *Options) {
		o.Logger = defaultLogger(val)
	}
}

func WithMemTableSize(val int64) Option {
	return func(o *Options) {
		o.MemTableSize = val
	}
}

func WithQueryAnalyzer(val bool) Option {
	return func(o *Options) {
		o.QueryAnalyzer = val
	}
}
