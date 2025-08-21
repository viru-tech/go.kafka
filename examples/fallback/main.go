package main

import (
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
	"github.com/rcrowley/go-metrics"
	kafka "github.com/viru-tech/go.kafka"
	"go.uber.org/zap"
)

type message struct {
	Data string `json:"data"`
}

func main() {
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal(err)
	}

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.ClientID = "example"
	// use your kafka version here
	kafkaConfig.Version = sarama.V2_6_0_0
	kafkaConfig.Metadata.Full = false

	kafkaConfig.Net.SASL.Enable = true
	kafkaConfig.Net.SASL.Version = sarama.SASLHandshakeV1
	kafkaConfig.Net.SASL.User = "go_services"
	kafkaConfig.Net.SASL.Password = "mDj75UrdgZYkr9WpkM6H"
	kafkaConfig.Producer.Compression = sarama.CompressionSnappy
	// it is necessary for metrics
	kafkaConfig.Producer.Return.Successes = true
	kafka.EnableMetrics(kafka.WithMetricsConstLabel("service", "example"))
	metrics.UseNilMetrics = true

	clusters := []string{
		"192.168.236.135:9092",
	}

	fallbackProducer, err := kafka.NewProducer(
		clusters,
		kafkaConfig,
		kafka.WithProducerLogger(logger),
	)
	if err != nil {
		logger.Error("failed to create fallback kafka producer", zap.Error(err))
		return
	}
	defer fallbackProducer.Close() //nolint:errcheck

	fsFallback := kafka.NewFSFallback("/tmp/kafka", kafka.WithFSFallbackLogger(logger))

	// fsResend will be read directory "/tmp/kafka" and resend to resendProducer saved messages from fsFallback.
	resendProducer, err := kafka.NewProducer(
		clusters,
		kafkaConfig,
		kafka.WithProducerLogger(logger),
	)
	if err != nil {
		logger.Error("failed to create resend kafka producer", zap.Error(err))
		return
	}

	fsResend := kafka.NewFSResend("/tmp/kafka",
		resendProducer,
		kafka.WithFSResendRetryInterval(time.Minute),
		kafka.WithFSResendLogger(logger),
	)
	defer fsResend.Close() //nolint:errcheck

	fsResend.Run()

	producer, err := kafka.NewProducer(
		clusters,
		kafkaConfig,
		kafka.WithProducerLogger(logger),
		kafka.WithFallbackChain(fallbackProducer, fsFallback),
	)
	if err != nil {
		logger.Error("failed to create kafka producer", zap.Error(err))
		return
	}
	defer producer.Close() //nolint:errcheck

	go func() {
		for {
			fmt.Println("cluster", producer.ClusterHealth())
			fmt.Println("topics", producer.TopicHealth("myTopic111111"))
			time.Sleep(5 * time.Second)
		}
	}()

	msg := message{Data: "Hello, world!"}
	if err := producer.SendJSON("myTopic", sarama.StringEncoder("myKey"), &msg); err != nil {
		logger.Error("failed to send json into producer", zap.Error(err))
	}
	<-make(chan interface{})
}
