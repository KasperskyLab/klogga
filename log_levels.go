package klogga

// LogLevel log levels simplify compatibility with some logging systems
type LogLevel int

func (l LogLevel) String() string {
	switch l {
	case Debug:
		return "D"
	case Info:
		return "I"
	case Warn:
		return "W"
	case Error:
		return "E"
	case Fatal:
		return "F"
	default:
		return "U"
	}
}

const (
	Debug LogLevel = -1
	Info  LogLevel = 0
	Warn  LogLevel = 1
	Error LogLevel = 2
	Fatal LogLevel = 3
)
