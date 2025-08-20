package main

import (
	"errors"
	"log"
	"os"

	"github.com/IBM/sarama"
	"github.com/jessevdk/go-flags"
	kafka "github.com/viru-tech/go.kafka"
	"go.uber.org/zap"
)

func main() {
	var config appConfig
	if _, err := flags.Parse(&config); err != nil {
		var flagsErr *flags.Error
		if errors.As(err, &flagsErr) && flagsErr.Type == flags.ErrHelp {
			os.Exit(0)
		}
		log.Fatalf("failed to parse config: %v", err)
	}

	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatal(err)
	}
	defer logger.Sync() //nolint:errcheck

	kafkaConfig := sarama.NewConfig()
	kafkaConfig.ClientID = config.Kafka.ClientID
	// use your kafka version here
	kafkaConfig.Version = sarama.V2_6_0_0
	kafkaConfig.Producer.Compression = sarama.CompressionSnappy

	resendProducer, err := kafka.NewProducer(
		config.Kafka.Brokers,
		kafkaConfig,
		kafka.WithProducerLogger(logger),
	)
	if err != nil {
		logger.Error("failed to create resend kafka producer", zap.Error(err))
		return
	}

	fsResend := kafka.NewFSResend(config.Directory,
		resendProducer,
		kafka.WithFSResendLogger(logger),
	)
	defer fsResend.Close() //nolint:errcheck

	if err := fsResend.Resend(); err != nil {
		logger.Error("failed to run resend", zap.Error(err))
		return
	}
}

type appConfig struct {
	Directory string      `long:"dir"        description:"Directory with fallback messages" required:"true"`
	Kafka     configKafka `namespace:"kafka" group:"Kafka configuration"`
}

type configKafka struct {
	ClientID string   `default:"resend"     long:"client_id" description:"Kafka client ID"`
	Brokers  []string `default:"kafka:9092" long:"brokers"   description:"List of brokers to connect to"`
}
