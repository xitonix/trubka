package kafka

import (
	"github.com/xitonix/trubka/internal"
)

type printerMock struct {
}

func (p *printerMock) Errorf(level internal.VerbosityLevel, format string, args ...interface{}) {
}

func (p *printerMock) Error(level internal.VerbosityLevel, msg string) {
}

func (p *printerMock) Infof(level internal.VerbosityLevel, format string, args ...interface{}) {
}

func (p *printerMock) Info(level internal.VerbosityLevel, msg string) {
}

func (p *printerMock) Warningf(level internal.VerbosityLevel, format string, args ...interface{}) {
}

func (p *printerMock) Warning(level internal.VerbosityLevel, msg string) {
}

func (p *printerMock) WriteEvent(topic string, bytes []byte) {
}

func (p *printerMock) Close() {
}

func (p *printerMock) Level() internal.VerbosityLevel {
	return internal.Verbose
}
