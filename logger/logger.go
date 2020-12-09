package logger

import (
	"context"
	"os"

	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

type (
	key string

	// Logger type embeds zap and also contains the current system name (namespace, Ns)
	Logger struct {
		*zap.Logger
		Ns string
	}

	// KConfig type for creating a new Kafka logger. Takes a Namespace,
	// Broker (eg 'localhost:9092'), Topic (eg 'topic-a')
	KConfig struct {
		Namespace string
		Broker    string
		Topic     string
		Async     bool
	}

	producerInterface interface {
		WriteMessages(ctx context.Context, msgs ...kafka.Message) error
	}

	// KafkaProducer contains a kafka.Producer and Kafka topic
	KafkaProducer struct {
		Producer producerInterface
		Topic    string
	}
)

const (
	// UID - uniquely request identifier
	UID key = "request_id"
)

var customConfig = zapcore.EncoderConfig{
	TimeKey:        "timeStamp",
	LevelKey:       "level",
	NameKey:        "logger",
	CallerKey:      "caller",
	FunctionKey:    zapcore.OmitKey,
	MessageKey:     "msg",
	StacktraceKey:  "stacktrace",
	LineEnding:     zapcore.DefaultLineEnding,
	EncodeLevel:    zapcore.CapitalColorLevelEncoder,
	EncodeTime:     zapcore.ISO8601TimeEncoder,
	EncodeDuration: zapcore.SecondsDurationEncoder,
}

// CInfo this function takes a context as first parameter, extracts specific fields as well as namespace, and calls zap Info
func (l *Logger) CInfo(ctx context.Context, msg string, fields ...zap.Field) {
	l.Info(msg, consolidate(ctx, l.Ns, fields...)...)
}

func consolidate(ctx context.Context, namespace string, fields ...zap.Field) []zap.Field {
	return append(append(ctxToZapFields(ctx), fields...), zap.String("ns", namespace))
}

// See advanced config example: https://github.com/uber-go/zap/blob/master/example_test.go#L105
var lowPriority = zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
	return lvl < zapcore.ErrorLevel && lvl > zapcore.DebugLevel
})
var debugPriority = zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
	return lvl < zapcore.ErrorLevel
})
var kafkaPriority = zap.LevelEnablerFunc(func(lvl zapcore.Level) bool {
	return lvl > zapcore.DebugLevel
})

// Init creates a new instance of a logger. Namespace is the name of the module using the logger. broker and topic are Kafa specific,
// if either of these is not set a default console logger is created.
func Init(namespace, broker, topic string, debug, async bool) *Logger {
	var kp *KafkaProducer = nil
	if broker != "" && topic != "" {
		kp = NewKafkaProducer(&KConfig{
		Broker: broker,
		Topic:  topic,
		Async:  async,
	})
	}
	logger := getLogger(debug, kp)
	// logger.Info("initiated logger", zap.String("ns", namespace), zap.Bool("kafka", kp != nil), zap.Bool("debug", debug))
	return &Logger{logger, namespace}
}

func getLogger(debug bool, kp *KafkaProducer) *zap.Logger {
	// cores are logger interfaces
	var cores []zapcore.Core

	// optimise message for console output (human readable)
	consoleEncoder := zapcore.NewConsoleEncoder(customConfig)
	// Lock wraps a WriteSyncer in a mutex to make it safe for concurrent use.
	// See https://godoc.org/go.uber.org/zap/zapcore
	cores = append(cores,
		zapcore.NewCore(consoleEncoder, zapcore.Lock(os.Stdout), getPriority(debug)),
		zapcore.NewCore(consoleEncoder, zapcore.Lock(os.Stderr), zap.ErrorLevel),
	)

	if kp != nil {
		cores = append(cores, zapcore.NewCore(zapcore.NewJSONEncoder(customConfig), zapcore.Lock(zapcore.AddSync(kp)), kafkaPriority))
	}

	// join inputs, encoders, level-handling functions into cores, then "tee" together
	logger := zap.New(zapcore.NewTee(cores...))
	defer logger.Sync()
	return logger
}

func getPriority(debug bool) zap.LevelEnablerFunc {
	if debug {
		return debugPriority
	}
	return lowPriority
}

func ctxToZapFields(ctx context.Context) []zap.Field {
	reqID, _ := ctx.Value(UID).(string)
	return []zap.Field{
		zap.String("UID", reqID),
	}
}

// NewKafkaProducer instantiates a kafka.Producer, saves topic, and returns a KafkaProducer
func NewKafkaProducer(c *KConfig) *KafkaProducer {
	return &KafkaProducer{
		Producer: kafka.NewWriter(kafka.WriterConfig{
			Brokers:      []string{c.Broker},
			Topic:        c.Topic,
			Balancer:     &kafka.Hash{},
			Async:        c.Async,
			RequiredAcks: -1, // -1 = all
		}),
		Topic: c.Topic,
	}
}

// Write takes a message as a byte slice, wraps in a kafka.message and calls kafka Produce
func (kp *KafkaProducer) Write(msg []byte) (int, error) {
	return len(msg), kp.Producer.WriteMessages(context.Background(), kafka.Message{
		Key:   []byte(""),
		Value: msg,
	})
}
