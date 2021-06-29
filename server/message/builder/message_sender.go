package builder

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/hopkings2008/yigfs/server/helper"
	"github.com/hopkings2008/yigfs/server/message/types"
	. "github.com/hopkings2008/yigfs/server/error"
)


var (
	MsgSender MessageSender
	initialized uint32
	mu sync.Mutex
)

type MessageSender interface {
	// send the message async
	AsyncSend(msg *types.Message) error
	// flush all the messages, timeout is in ms.
	Flush(timeout int) error
	// free this instance.
	Close()
}

// create the singleton MessageSender
func GetMessageSender() (MessageSender, error) {
	var err error
	if atomic.LoadUint32(&initialized) == 1 {
		return MsgSender, nil
	}
	mu.Lock()
	defer mu.Unlock()
	if initialized == 0 {
		builder, ok := MessageProducerBuilder[helper.CONFIG.KafkaConfig.Type]
		if !ok {
			helper.Logger.Error(nil, fmt.Sprintf("GetMessageSender: KafkaConfig is invalidate, type is: %v", helper.CONFIG.KafkaConfig.Type))
			return nil, errors.New("KafkaConfig is invalidate.")
		}

		MsgSender, err = builder.Create(helper.CONFIG.KafkaConfig)
		if err != nil || nil == MsgSender {
			helper.Logger.Error(nil, fmt.Sprintf("Failed to create message sender, err: %v", err))
			return nil, ErrYigFsFailedCreateMessageProducer
		}

		atomic.StoreUint32(&initialized, 1)
	}
	
	return MsgSender, nil
}

// send message
func SendMessage(topic, key string, value []byte) error {
	sender, err := GetMessageSender()
	if err != nil {
		helper.Logger.Error(nil, fmt.Sprintf("Failed to get message sender, err: %v", err))
		return ErrYigFsFailedCreateMessageProducer
	}

	msg := &types.Message{
		Topic:   topic,
		Key:     key,
		ErrChan: nil,
		Value:   value,
	}

	err = sender.AsyncSend(msg)
	if err != nil {
		helper.Logger.Error(nil, fmt.Sprintf("Failed to send message to kafka with err: %v", err))
		return ErrYigFsFailedToSendMessage
	}

	return nil
}

// create topic
func CreateTopics(topic string, partNum int) (err error) {
	builder, ok := MessageProducerBuilder[helper.CONFIG.KafkaConfig.Type]
	if !ok {
		helper.Logger.Error(nil, fmt.Sprintf("CreateTopics: KafkaConfig is invalidate, type is: %v", helper.CONFIG.KafkaConfig.Type))
		return errors.New("KafkaConfig is invalidate.")
	}

	err = builder.CreateTopic(helper.CONFIG.KafkaConfig, topic, partNum)
	if err != nil {
		return
	}
	
	return nil
}