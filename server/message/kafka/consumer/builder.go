package consumer

import (
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hopkings2008/yigfs/server/helper"
)

type KafkaConsumerBuilder struct {
}

func (kb *KafkaConsumerBuilder) Create(config helper.KafkaConfig, groupId string) (*Consumer, error) {
	server := config.Server
	if server == "" {
		helper.Logger.Error(nil, fmt.Sprintf("The KafkaConfig's server is invalid, server: %v", server))
		return nil, errors.New("The KafkaConfig's server is invalid")
	}

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap {
		"bootstrap.servers":               server,
		"broker.address.family":           "v4",
		"group.id":                        groupId,
		"session.timeout.ms":              6000,
		"max.poll.interval.ms":            18000000,
		"enable.auto.commit":              false,
		"go.application.rebalance.enable": true,
		"go.events.channel.enable":        true,
		"enable.partition.eof":            true,
		"enable.auto.offset.store": 	   false,
		"auto.offset.reset":               "earliest"})
	if err != nil {
		return nil ,err
	}

	c := &Consumer {
		Consumer: consumer,
		EndQ: make(chan bool),
	}

	helper.Logger.Info(nil, fmt.Sprintf("Succeed to create consumer, groupId: %v", groupId))
	return c, nil
}
