package producer

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hopkings2008/yigfs/server/helper"
	"github.com/hopkings2008/yigfs/server/message/builder"
)

type KafkaProducerBuilder struct {
}

func (kb *KafkaProducerBuilder) Create(config helper.KafkaConfig) (builder.MessageSender, error) {
	autoOffsetStore := false
	server := config.Server
	if server == "" {
		helper.Logger.Error(nil, "The KafkaConfig's server is invalid, server is nil")
		return nil, errors.New("The KafkaConfig's server is invalid")
	}

	p, err := kafka.NewProducer(&kafka.ConfigMap {
		"bootstrap.servers":        server,
		"enable.auto.offset.store": autoOffsetStore,
	})
	if err != nil {
		return nil, err
	}

	producer := &Producer {
		producer: p,
		doneChan: make(chan int),
	}

	producer.Start()

	return producer, nil
}

func (kb *KafkaProducerBuilder) CreateTopic(config helper.KafkaConfig, topic string, partNum int) error {
	broker := config.Server
	if broker == "" {
		helper.Logger.Error(nil, "CreateTopic: The KafkaConfig's server is invalid, server is nil")
		return errors.New("The KafkaConfig's server is invalid")
	}

	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers":        broker,
		"request.timeout.ms":       6000,
		"message.timeout.ms":       6000,
		"message.send.max.retries": 4,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to new admin client, err: %v", err))
		return err
	}

	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		return err
	}
	_, err = adminClient.CreateTopics(ctx,
		[]kafka.TopicSpecification{{
			Topic:             topic,
			NumPartitions:     partNum,
			ReplicationFactor: 1}},
		kafka.SetAdminOperationTimeout(maxDur))
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to create topics, err: %v", err))
		return err
	}
	
	adminClient.Close()

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to create topics, topic: %v, partNum: %v", topic, partNum))
	return nil
}
