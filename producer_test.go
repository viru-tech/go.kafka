package go_kafka

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

//nolint:tparallel
func TestProducer_Fallback(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup

	mainAsyncPSuccess := make(chan *sarama.ProducerMessage, 1)
	mainAsyncPErrors := make(chan *sarama.ProducerError, 1)
	mainAsyncPInput := make(chan *sarama.ProducerMessage, 1)
	fallbackAsyncPSuccess := make(chan *sarama.ProducerMessage, 1)
	fallbackAsyncPErrors := make(chan *sarama.ProducerError, 1)
	fallbackAsyncPInput := make(chan *sarama.ProducerMessage, 1)

	ctrl := gomock.NewController(t)
	s3Mock := NewMockFallback(ctrl)
	fsMock := NewMockFallback(ctrl)
	admin := NewMockClient(ctrl)

	fallbackAsyncP := NewMockSaramaAsyncProducer(ctrl)
	fallbackAsyncP.EXPECT().Input().Return(fallbackAsyncPInput).AnyTimes()
	fallbackAsyncP.EXPECT().Successes().Return(fallbackAsyncPSuccess).AnyTimes()
	fallbackAsyncP.EXPECT().Errors().Return(fallbackAsyncPErrors).AnyTimes()
	mainAsyncP := NewMockSaramaAsyncProducer(ctrl)
	mainAsyncP.EXPECT().Input().Return(mainAsyncPInput).AnyTimes()
	mainAsyncP.EXPECT().Successes().Return(mainAsyncPSuccess).AnyTimes()
	mainAsyncP.EXPECT().Errors().Return(mainAsyncPErrors).AnyTimes()

	fallbackP := newProducer(fallbackAsyncP, admin, WithFallbackChain(s3Mock, fsMock))
	mainP := newProducer(mainAsyncP, admin, WithFallback(fallbackP))

	msg := &sarama.ProducerMessage{
		Topic: "topic",
		Key:   sarama.ByteEncoder("key"),
		Value: sarama.ByteEncoder("value"),
		Headers: []sarama.RecordHeader{
			{
				Key:   []byte("ua"),
				Value: []byte("postman"),
			},
		},
		Metadata: map[string]string{
			"foo": "bar",
		},
	}

	t.Run("complex fallback full", func(t *testing.T) {
		wg.Add(1)

		sendErr := mainP.SendMessage(msg)
		require.NoError(t, sendErr)

		// main producer attempts to send message and gets error
		actual := <-mainAsyncPInput
		require.Same(t, msg, actual)
		err := &sarama.ProducerError{
			Msg: msg,
			Err: errors.New("fake error"),
		}
		mainAsyncPErrors <- err

		// fallback producer is next to try, but error will
		// cause s3 and filesystem fallbacks to trigger
		s3Mock.EXPECT().SaveMessage(gomock.Any(), msg).Return(errors.New("s3 error"))
		fsMock.EXPECT().SaveMessage(gomock.Any(), msg).DoAndReturn(
			func(_ context.Context, _ *sarama.ProducerMessage) error {
				wg.Done()
				return nil
			},
		)

		actual = <-fallbackAsyncPInput
		require.Same(t, msg, actual)
		fallbackAsyncPErrors <- err
	})

	wg.Wait()
}

//nolint:tparallel
func TestProducer_Close(t *testing.T) {
	t.Parallel()

	var wg sync.WaitGroup

	ctrl := gomock.NewController(t)
	mainAsyncP := NewMockSaramaAsyncProducer(ctrl)
	admin := NewMockClient(ctrl)

	mainAsyncPSuccess := make(chan *sarama.ProducerMessage, 1)
	mainAsyncPErrors := make(chan *sarama.ProducerError, 1)
	mainAsyncP.EXPECT().Successes().Return(mainAsyncPSuccess).AnyTimes()
	mainAsyncP.EXPECT().Errors().Return(mainAsyncPErrors).AnyTimes()
	mainAsyncP.EXPECT().AsyncClose().Do(func() {
		close(mainAsyncPSuccess)
		close(mainAsyncPErrors)
	})

	admin.EXPECT().Close()

	mainP := newProducer(mainAsyncP, admin)

	t.Run("don't send the messages while producer is closing", func(t *testing.T) {
		wg.Add(1)
		defer wg.Done()

		closeErr := mainP.Close()
		require.NoError(t, closeErr)

		err := mainP.SendMessage(&sarama.ProducerMessage{})
		require.ErrorIs(t, ErrClosing, err)

		err = mainP.SendBytes("my_topic", sarama.ByteEncoder("key"), sarama.ByteEncoder("value"))
		require.ErrorIs(t, ErrClosing, err)
	})

	wg.Wait()
}
