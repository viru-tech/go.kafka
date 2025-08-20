package go_kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/IBM/sarama"
	jsoniter "github.com/json-iterator/go"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// ErrClosing returned when the producer is closing.
var ErrClosing = errors.New("cannot send the message: producer is closing")

//go:generate mockgen -source=producer.go -package=kafka -destination=producer_mock.go
//go:generate mockgen -source=./vendor/github.com/IBM/sarama/async_producer.go -imports=sarama=github.com/IBM/sarama -package=kafka -mock_names=AsyncProducer=MockSaramaAsyncProducer -destination=async_producer_mock_test.go
//go:generate mockgen -source=./vendor/github.com/IBM/sarama/client.go -imports=sarama=github.com/IBM/sarama -package=kafka -mock_names=Admin=MockClient -destination=client_mock_test.go

// AsyncProducer is a wrapper over sarama.AsyncProducer
// with SendXXX methods we use. All the complexity of sarama's
// is hidden behind this interface implementation.
type AsyncProducer interface {
	// Closer closes underlying async producer, waiting
	// for any pending messages to be flushed and allocated
	// resources (goroutines, etc) released.
	io.Closer

	// SendMessage pushes passed message to the underlying
	// Kafka's asynchronous producer.
	SendMessage(msg *sarama.ProducerMessage) error
	// SendBytes simply pushes passed bytes to the underlying
	// Kafka's asynchronous producer.
	SendBytes(topic string, key sarama.Encoder, data []byte) error
	// SendJSON sends binary JSON representation of the passed
	// arbitrary message to the Kafka in an asynchronous way.
	SendJSON(topic string, key sarama.Encoder, v interface{}) error
	// SendProto sends binary representation of the passed proto
	// message to the Kafka in an asynchronous way.
	SendProto(topic string, key sarama.Encoder, v proto.Message) error
}

// Producer is a wrapper for sarama.AsyncProducer, which
// implements some convenient features such as fallback.
type Producer struct {
	producer sarama.AsyncProducer
	client   sarama.Client
	fallback Fallback
	logger   *zap.Logger

	// gets closed when the shutdown is initiated
	messagesMu *sync.RWMutex

	// unblocks after all status channels are closed
	statusGroup     *sync.WaitGroup
	fallbackTimeout time.Duration
}

// NewProducer returns ready-to-use new Producer that will
// interact with the passed brokers for message sending.
// For producers with fallbacks make sure to enable
// sarama.Config.Producer.Return.Errors.
// If collection of metrics is enabled make sure to enable
// sarama.Config.Producer.Return.Successes.
func NewProducer(
	brokers []string,
	conf *sarama.Config,
	opts ...ProducerOption,
) (*Producer, error) {
	client, err := sarama.NewClient(brokers, conf)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("failed to create async producer: %w", err)
	}

	return newProducer(producer, client, opts...), nil
}

func newProducer(producer sarama.AsyncProducer, client sarama.Client, opts ...ProducerOption) *Producer {
	p := &Producer{
		logger:          zap.NewNop(),
		producer:        producer,
		client:          client,
		fallbackTimeout: time.Millisecond * 500,
		messagesMu:      new(sync.RWMutex),
		statusGroup:     new(sync.WaitGroup),
	}

	for _, opt := range opts {
		opt(p)
	}

	p.watchSuccesses()
	p.watchErrors()
	return p
}

// Close closes underlying async producer, waiting
// for any pending messages to be flushed and allocated
// resources (goroutines, etc) released.
func (p *Producer) Close() error {
	p.messagesMu.Lock()
	p.producer.AsyncClose()
	p.statusGroup.Wait()

	return p.client.Close()
}

// SendMessage pushes passed message to the underlying
// Kafka's asynchronous producer.
func (p *Producer) SendMessage(msg *sarama.ProducerMessage) error {
	if !p.messagesMu.TryRLock() {
		return ErrClosing
	}

	p.producer.Input() <- msg

	p.messagesMu.RUnlock()

	return nil
}

// SaveMessage allows using producer p as a Fallback. It simply
// pushes msg to the configured brokers. No error is ever returned,
// fallback should be configured for p to handle async producer errors.
func (p *Producer) SaveMessage(_ context.Context, msg *sarama.ProducerMessage) error {
	return p.SendMessage(msg)
}

// SendBytes simply pushes passed bytes to the underlying
// Kafka's asynchronous producer.
func (p *Producer) SendBytes(topic string, key sarama.Encoder, data []byte) error {
	return p.SendMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   key,
		Value: sarama.ByteEncoder(data),
	})
}

// SendJSON sends binary JSON representation of the passed
// arbitrary message to the Kafka in an asynchronous way.
func (p *Producer) SendJSON(topic string, key sarama.Encoder, v interface{}) error {
	data, err := jsoniter.Marshal(v)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	return p.SendBytes(topic, key, data)
}

// SendProto sends binary representation of the passed proto
// message to the Kafka in an asynchronous way.
func (p *Producer) SendProto(topic string, key sarama.Encoder, v proto.Message) error {
	data, err := proto.Marshal(v)
	if err != nil {
		return fmt.Errorf("failed to marshal data: %w", err)
	}

	return p.SendBytes(topic, key, data)
}

func (p *Producer) watchSuccesses() {
	if p.producer.Successes() == nil {
		return
	}

	p.statusGroup.Add(1)

	go func() {
		defer p.statusGroup.Done()

		for msg := range p.producer.Successes() {
			incProducerSentCounter(msg.Topic, false)
		}
	}()
}

func (p *Producer) watchErrors() {
	if p.producer.Errors() == nil {
		return
	}

	p.statusGroup.Add(1)

	go func() {
		defer p.statusGroup.Done()

		for pErr := range p.producer.Errors() {
			p.logger.Error("kafka error", zap.Error(pErr.Err))
			incProducerSentCounter(pErr.Msg.Topic, true)

			if p.fallback != nil {
				ctx, cancel := context.WithTimeout(context.Background(), p.fallbackTimeout)
				if err := p.fallback.SaveMessage(ctx, pErr.Msg); err != nil {
					p.logger.Error("failed to save message", zap.Error(err))
				}
				cancel()
			}
		}
	}()
}
