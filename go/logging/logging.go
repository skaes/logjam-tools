package logging

import (
	"fmt"
	"os"
)

func Info(format string, args ...interface{}) {
	_format := "[I] " + format + "\n"
	fmt.Printf(_format, args...)
}

func Warn(format string, args ...interface{}) {
	_format := "[W] " + format + "\n"
	fmt.Fprintf(os.Stderr, _format, args...)
}

func Error(format string, args ...interface{}) {
	_format := "[E] " + format + "\n"
	fmt.Fprintf(os.Stderr, _format, args...)
}

func Fatal(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	panic(msg)
}
