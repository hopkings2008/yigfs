package producer

import (
	"errors"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hopkings2008/yigfs/server/helper"
	"github.com/hopkings2008/yigfs/server/message/types"
)


type Producer struct {
	producer *kafka.Producer
	doneChan chan int
}

func( p *Producer) Start() error {
	if p.producer == nil {
		helper.Logger.Error(nil, "Start: Kafka producer is not created correclty yet.")
		return errors.New("Kafka producer is not created correctly.")
	}

	go func() {
		helper.Logger.Info(nil, "kafka producer start")
		defer close(p.doneChan)
		for e := range p.producer.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				m := ev
				if m.Opaque != nil {
					switch v := m.Opaque.(type) {
					case chan error:
						if v != nil {
							go func(c chan error, err error) {
								c <- err
							}(v, m.TopicPartition.Error)
						}
					}
				}
				if m.TopicPartition.Error != nil {
					// error here.
					helper.Logger.Error(nil, fmt.Sprintf("Failed to send message to topic[%s] [%d] at offset [%v] with err: %v",
						*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset, m.TopicPartition.Error))
					break
				}

				helper.Logger.Info(nil, fmt.Sprintf("Succeed to send message to topic[%s] [%d] at offset [%v]",
					*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset))
			default:
				helper.Logger.Info(nil, "Skip event:", ev)
			}
		}
	}()

	return nil
}

func (p *Producer) Flush(timeout int) error {
	if p.producer == nil {
		helper.Logger.Error(nil, "Flush: Kafka producer is not created correclty yet.")
		return errors.New("Kafka producer is not created correclty yet.")
	}
	p.producer.Flush(timeout)
	return nil
}

func(p *Producer) Close() {
	if p.producer == nil {
		return
	}

	p.producer.Flush(300000)
	p.producer.Close()
	_ = <- p.doneChan
}

func(p *Producer) AsyncSend(msg *types.Message) error {
	if p.producer == nil {
		helper.Logger.Error(nil, "AsyncSend: Kafka producer is not created correclty yet.")
		return errors.New("Kafka producer is not created correclty yet.")
	}
	
	if msg.Value == nil || msg.Key == "" || msg.Topic == "" {
		helper.Logger.Error(nil, fmt.Sprintf("Input message[%v] is invalid.", msg))
		return errors.New(fmt.Sprintf("Input message[%v] is invalid.", msg))
	}

	key := []byte(msg.Key)
	p.producer.ProduceChannel() <- &kafka.Message {
		TopicPartition: kafka.TopicPartition {
			Topic: &msg.Topic,
			Partition: kafka.PartitionAny,
		},
		Key: key,
		Value: msg.Value,
		Opaque: msg.ErrChan,
	}

	return nil
}