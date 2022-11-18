package borm

type Options struct {
	Logger Logger
}

type Option func(*Options)

func newOptions(ops ...Option) *Options {
	opt := &Options{
		Logger: defaultLogger(WARNING),
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
