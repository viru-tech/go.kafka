package go_kafka

import (
	"errors"
	"fmt"

	"github.com/IBM/sarama"
)

// ErrNoActiveBrokers is the error returned when we weren't been able
// to connect to any brokers initially.
var ErrNoActiveBrokers = errors.New("no active brokers")

// TopicHealth returns health check for topics.
func (p *Producer) TopicHealth(names ...string) error {
	return p.doOnAnyBroker(func(b *sarama.Broker) error {
		topics, err := b.GetMetadata(&sarama.MetadataRequest{
			Topics: names,
		})
		if err != nil || len(topics.Topics) != len(names) {
			return err
		}

		var errs []error
		for _, topic := range topics.Topics {
			if !errors.Is(topic.Err, sarama.ErrNoError) {
				errs = append(errs, fmt.Errorf("topic %s error: %s", topic.Name, topic.Err.Error())) //nolint:err113
			}
		}

		return errors.Join(errs...)
	})
}

// ClusterHealth returns health check for cluster.
func (p *Producer) ClusterHealth() error {
	return p.doOnAnyBroker(func(b *sarama.Broker) error {
		_, err := b.ApiVersions(&sarama.ApiVersionsRequest{})
		return err
	})
}

func (p *Producer) doOnAnyBroker(function func(b *sarama.Broker) error) error {
	err := p.client.RefreshMetadata()
	if err != nil {
		return err
	}

	brokers := p.client.Brokers()
	if len(brokers) == 0 {
		return ErrNoActiveBrokers
	}

	for i := range brokers {
		err = function(brokers[i])
		if err == nil {
			return nil
		}
	}

	return err
}
