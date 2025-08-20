package go_kafka

import (
	"context"
	"fmt"
	"os"

	"github.com/IBM/sarama"
	"github.com/hashicorp/go-multierror"
	jsoniter "github.com/json-iterator/go"
	"go.uber.org/zap"
)

//go:generate mockgen -source=fallback.go -package=kafka -destination=fallback_mock.go

const fsFallbackType = "fs"

// Fallback is used when Kafka is no longer available.
// Producer will use fallback to persist message it could
// not have sent due to Kafka failure.
type Fallback interface {
	// SaveMessage saves passed message somewhere so that
	// it is not lost due to Kafka failure.
	SaveMessage(ctx context.Context, msg *sarama.ProducerMessage) error
}

// FallbackChain allows chaining fallbacks so that if preceding
// fallback returns an error, next one will be used. If all fallbacks
// in a chain fail, FallbackChain generates its own error.
type FallbackChain []Fallback

// SaveMessage attempts to apply fallback mechanisms in the chain
// in order. If any of fallback succeeds no error is returned.
func (ff FallbackChain) SaveMessage(ctx context.Context, msg *sarama.ProducerMessage) error {
	var multiErr error
	for _, f := range ff {
		if err := f.SaveMessage(ctx, msg); err != nil {
			multiErr = multierror.Append(multiErr, err)
		} else {
			return nil
		}
	}

	return fmt.Errorf("failed to save message in fallback chain: %w", multiErr)
}

// FallbackMessage is what we persist on filesystem instead of
// sarama.ProducerMessage. Specific Fallback implementations may
// use their own format instead.
type FallbackMessage struct {
	Topic    string                `json:"topic"`
	Metadata interface{}           `json:"metadata,omitempty"`
	Key      []byte                `json:"key,omitempty"`
	Value    []byte                `json:"value"`
	Headers  []sarama.RecordHeader `json:"headers,omitempty"`
}

// NewFallbackMessage constructs FallbackMessage from the passed msg.
func NewFallbackMessage(msg *sarama.ProducerMessage) (*FallbackMessage, error) {
	var (
		key, value []byte
		err        error
	)

	if msg.Key != nil {
		key, err = msg.Key.Encode()
		if err != nil {
			return nil, fmt.Errorf("failed to encode key: %w", err)
		}
	}

	value, err = msg.Value.Encode()
	if err != nil {
		return nil, fmt.Errorf("failed to encode value: %w", err)
	}

	return &FallbackMessage{
		Topic:    msg.Topic,
		Key:      key,
		Value:    value,
		Headers:  msg.Headers,
		Metadata: msg.Metadata,
	}, nil
}

// FSFallback is a filesystem fallback. It persists messages
// as a separate files in the base directory, pointed by
// FSFallback string value. It automatically tries to resend
// saved messages once in a while.
type FSFallback struct {
	logger *zap.Logger

	dir string
}

// NewFSFallback returns new filesystem fallback, that will save passed messages
// as a separate files in dir and optionally resend them (if configured).
func NewFSFallback(dir string, opts ...FSFallbackOption) *FSFallback {
	f := &FSFallback{
		dir:    dir,
		logger: zap.NewNop(),
	}

	for _, opt := range opts {
		opt(f)
	}

	return f
}

// SaveMessage saves JSON-representation of the passed message in a new file with unique name.
func (f *FSFallback) SaveMessage(_ context.Context, msg *sarama.ProducerMessage) error {
	var err error
	defer func() {
		incFallbackSavedCounter(fsFallbackType, msg.Topic, err != nil)
	}()

	data, err := NewFallbackMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to create fallback message: %w", err)
	}

	out, err := os.CreateTemp(f.dir, "")
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer out.Close() //nolint:errcheck

	if err = jsoniter.NewEncoder(out).Encode(data); err != nil {
		return fmt.Errorf("failed to encode message to file: %w", err)
	}

	if err := out.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	f.logger.Debug("saved kafka message", zap.String("topic", msg.Topic), zap.String("file", out.Name()))

	return nil
}
