package builder

import (
	"github.com/hopkings2008/yigfs/server/helper"
)


var (
	MessageProducerBuilder = make(map[int]MessageSenderBuilder)
)

type MessageSenderBuilder interface {
	Create(config helper.KafkaConfig) (MessageSender, error)
	CreateTopic(config helper.KafkaConfig, topic string, partNum int) error
}

func AddMsgSenderBuilder(builderType int, builder MessageSenderBuilder) {
	MessageProducerBuilder[builderType] = builder
}

