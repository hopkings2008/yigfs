package consumer

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hopkings2008/yigfs/server/helper"
)

type Consumer struct {
	Consumer *kafka.Consumer
	EndQ chan bool
}

func(c *Consumer) SubscribeTopics(topic []string) error {
	err := c.Consumer.SubscribeTopics(topic, nil)
	if err != nil {
		helper.Logger.Error(nil, fmt.Sprintf("Failed to subscribe topics, topics: %v, err: %v", topic, err))
		return err
	}

	helper.Logger.Info(nil, fmt.Sprintf("Succeed to subscribe topics, topics: %v, err: %v", topic, err))
	return nil
}

func(c *Consumer) Close() {
	c.Consumer.Close()
}


