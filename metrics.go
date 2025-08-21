package go_kafka

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

//nolint:gochecknoglobals
var (
	producerSentCounter    *prometheus.CounterVec
	fallbackSavedCounter   *prometheus.CounterVec
	resendUnprocessedGauge *prometheus.GaugeVec
	resendSentCounter      *prometheus.CounterVec
)

// MetricsOpts contains metrics configuration.
type MetricsOpts struct {
	ConstLabels map[string]string
}

// EnableMetrics creates a set of metrics and registers on a
// Prometheus registry.
func EnableMetrics(opts ...MetricsOption) {
	mOpts := &MetricsOpts{
		ConstLabels: make(map[string]string),
	}
	for _, o := range opts {
		o(mOpts)
	}

	producerSentCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "kafka_producer_sent_total",
			Help:        "Total number of messages sent to the kafka",
			ConstLabels: mOpts.ConstLabels,
		},
		[]string{"topic", "error"},
	)
	fallbackSavedCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "kafka_fallback_saved_total",
			Help:        "Total number of messages saved to fallback",
			ConstLabels: mOpts.ConstLabels,
		},
		[]string{"type", "topic", "error"},
	)
	resendUnprocessedGauge = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name:        "kafka_resend_unprocessed",
			Help:        "Number of messages that are currently unprocessed",
			ConstLabels: mOpts.ConstLabels,
		},
		[]string{"type"},
	)
	resendSentCounter = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name:        "kafka_resend_sent_total",
			Help:        "Total number of messages that were resent to the kafka",
			ConstLabels: mOpts.ConstLabels,
		},
		[]string{"type"},
	)
}

func incProducerSentCounter(topic string, isError bool) {
	if producerSentCounter != nil {
		producerSentCounter.WithLabelValues(topic, strconv.FormatBool(isError)).Inc()
	}
}

func incFallbackSavedCounter(fallbackType, topic string, isError bool) {
	if fallbackSavedCounter != nil {
		fallbackSavedCounter.WithLabelValues(fallbackType, topic, strconv.FormatBool(isError)).Inc()
	}
}

func setResendUnprocessedGauge(fallbackType string, value int) {
	if resendUnprocessedGauge != nil {
		resendUnprocessedGauge.WithLabelValues(fallbackType).Set(float64(value))
	}
}

func incResendSentCounter(fallbackType string) {
	if resendSentCounter != nil {
		resendSentCounter.WithLabelValues(fallbackType).Inc()
	}
}
