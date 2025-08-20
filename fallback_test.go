package go_kafka

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestFSFallback_SaveMessage(t *testing.T) {
	t.Parallel()

	tt := []struct {
		name   string
		in     *sarama.ProducerMessage
		expect string
	}{
		{
			name: "all fields",
			in: &sarama.ProducerMessage{
				Topic: "topic",
				Key:   sarama.StringEncoder("key"),
				Value: sarama.StringEncoder("value"),
				Headers: []sarama.RecordHeader{
					{
						Key:   []byte("ua"),
						Value: []byte("postman"),
					},
				},
				Metadata: map[string]string{
					"foo": "bar",
				},
			},
			expect: `{
				"topic": "topic",
				"key": "a2V5",
				"value": "dmFsdWU=",
				"metadata": {
					"foo": "bar"
				},
				"headers": [{"Key":"dWE=", "Value":"cG9zdG1hbg=="}]
			}`,
		},
		{
			name: "key and value only",
			in: &sarama.ProducerMessage{
				Topic: "topic",
				Key:   sarama.StringEncoder("key"),
				Value: sarama.StringEncoder("value"),
			},
			expect: `{
				"topic": "topic",
				"key": "a2V5",
				"value": "dmFsdWU="
			}`,
		},
		{
			name: "value only",
			in: &sarama.ProducerMessage{
				Topic: "topic",
				Value: sarama.StringEncoder("value"),
			},
			expect: `{
				"topic": "topic",
				"value": "dmFsdWU="
			}`,
		},
	}

	for i := range tt {
		tc := tt[i]
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			t.Cleanup(func() {
				err := os.RemoveAll(dir)
				require.NoError(t, err)
			})

			fsFallback := NewFSFallback(dir)

			err := fsFallback.SaveMessage(t.Context(), tc.in)
			require.NoError(t, err)

			dd, err := os.ReadDir(dir)
			require.NoError(t, err)
			require.Len(t, dd, 1)

			data, err := os.ReadFile(filepath.Join(dir, dd[0].Name()))
			require.NoError(t, err)
			require.JSONEq(t, tc.expect, string(data))
		})
	}
}

func TestFSFallback_resendMessages(t *testing.T) {
	t.Parallel()

	const retryInterval = time.Millisecond * 100
	msg := &sarama.ProducerMessage{
		Topic: "topic",
		Key:   sarama.ByteEncoder("key"),
		Value: sarama.ByteEncoder("value"),
	}

	dir := t.TempDir()
	t.Cleanup(func() {
		err := os.RemoveAll(dir)
		require.NoError(t, err)
	})

	ctrl := gomock.NewController(t)
	p := NewMockAsyncProducer(ctrl)
	p.EXPECT().SendMessage(msg)

	fsFallback := NewFSFallback(dir)

	fsResend := NewFSResend(dir, p, WithFSResendRetryInterval(retryInterval))
	t.Cleanup(func() {
		require.NoError(t, fsResend.Close())
	})
	fsResend.Run()

	err := fsFallback.SaveMessage(t.Context(), msg)
	require.NoError(t, err)

	dd, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, dd, 1)

	time.Sleep(2 * retryInterval)
	dd, err = os.ReadDir(dir)
	require.NoError(t, err)
	require.Empty(t, dd)
}
