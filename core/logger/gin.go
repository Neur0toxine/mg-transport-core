package logger

import (
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

// GinMiddleware will construct Gin middleware which will log requests and provide logger with unique request ID.
func GinMiddleware(log Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Start timer
		start := time.Now()
		path := c.Request.URL.Path
		raw := c.Request.URL.RawQuery

		streamID := generateStreamID()
		log := log.With(StreamID(streamID))
		c.Set(StreamIDAttr, streamID)
		c.Set("logger", log)

		// Process request
		c.Next()

		end := time.Now()
		if raw != "" {
			path = path + "?" + raw
		}

		log.Info("request",
			zap.String(HandlerAttr, "GIN"),
			zap.String("startTime", start.Format(time.RFC3339)),
			zap.String("endTime", end.Format(time.RFC3339)),
			zap.Any("latency", end.Sub(start)/time.Millisecond),
			zap.String("remoteAddress", c.ClientIP()),
			zap.String(HTTPMethodAttr, c.Request.Method),
			zap.String("path", path),
			zap.Int("bodySize", c.Writer.Size()),
		)
	}
}

func MustGet(c *gin.Context) Logger {
	return c.MustGet("logger").(Logger)
}
