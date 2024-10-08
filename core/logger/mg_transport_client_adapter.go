package logger

import (
	"fmt"

	"go.uber.org/zap"

	v1 "github.com/retailcrm/mg-transport-api-client-go/v1"
)

const (
	mgDebugLogReq     = "MG TRANSPORT API Request: %s %s %s %v"
	mgDebugLogReqFile = "MG TRANSPORT API Request: %s %s %s [file data]"
	mgDebugLogResp    = "MG TRANSPORT API Response: %s"
)

// MGTransportClientLogger implements both v1.BasicLogger and v1.DebugLogger.
type MGTransportClientLogger interface {
	v1.BasicLogger
	v1.DebugLogger
}

type mgTransportClientAdapter struct {
	log Logger
}

// MGTransportClientAdapter constructs an adapter that will log MG requests and responses.
func MGTransportClientAdapter(log Logger) MGTransportClientLogger {
	return &mgTransportClientAdapter{log: log}
}

// Debugf writes a message with Debug level.
func (m *mgTransportClientAdapter) Debugf(msg string, args ...interface{}) {
	var body interface{}
	switch msg {
	case mgDebugLogReqFile:
		body = "[file data]"
		fallthrough
	case mgDebugLogReq:
		var method, uri, token string
		if len(args) > 0 {
			method = fmt.Sprint(args[0])
		}
		if len(args) > 1 {
			uri = fmt.Sprint(args[1])
		}
		if len(args) > 2 { // nolint:gomnd
			token = fmt.Sprint(args[2])
		}
		if len(args) > 3 { // nolint:gomnd
			body = args[3]
		}
		m.log.Debug("MG TRANSPORT API Request",
			zap.String(HTTPMethodAttr, method), zap.String("url", uri),
			zap.String("token", token), Body(body))
	case mgDebugLogResp:
		m.log.Debug("MG TRANSPORT API Response", Body(args[0]))
	default:
		m.log.Debug(fmt.Sprintf(msg, args...))
	}
}

// Printf is a v1.BasicLogger implementation.
func (m *mgTransportClientAdapter) Printf(msg string, args ...interface{}) {
	m.Debugf(msg, args...)
}
