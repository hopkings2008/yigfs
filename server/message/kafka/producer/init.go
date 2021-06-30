package producer

import (
	"github.com/hopkings2008/yigfs/server/message/builder"
	"github.com/hopkings2008/yigfs/server/message/types"
)

func init() {
	KafkaProducerBuilder := &KafkaProducerBuilder{}
	builder.AddMsgSenderBuilder(types.MSG_BUS_SENDER_KAFKA, KafkaProducerBuilder)
}
