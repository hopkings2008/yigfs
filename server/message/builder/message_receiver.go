package builder

import (
	"errors"
	"fmt"

	"github.com/hopkings2008/yigfs/server/helper"
	"github.com/hopkings2008/yigfs/server/message/kafka/consumer"
)


// create the singleton MessageSender
func CreateMessageReceiver(groupId string) (*consumer.Consumer, error) {
	var err error
	builder := &consumer.KafkaConsumerBuilder{}
	MsgReceiver, err := builder.Create(helper.CONFIG.KafkaConfig, groupId)
	if err != nil || nil == MsgSender {
		helper.Logger.Error(nil, fmt.Sprintf("Failed to create message receiver, err: %v", err))
		return nil, errors.New(fmt.Sprintf("Failed to create message receiver, err: %v", err))
	}
	
	return MsgReceiver, nil
}
