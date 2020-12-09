package main

import (
	"context"

	"github.com/roppa/kafka-log/logger"
)

func main() {
	loggerA := logger.Init("test-service", "localhost:9092", "topica", false, false)
	loggerB := logger.Init("test-service", "localhost:9092", "topicb", false, true)

	ctx := context.Background()
	ctx2 := context.WithValue(ctx, logger.UID, "abc123")

	loggerA.CInfo(ctx2, "topic-a log 1")
	loggerB.CInfo(ctx2, "topic-b log 1")

	loggerA.CInfo(ctx2, "topic-a log 2")
	loggerB.CInfo(ctx2, "topic-b log 2")

	loggerA.CInfo(ctx2, "topic-a log 3")
	loggerB.CInfo(ctx2, "topic-b log 3")
}
