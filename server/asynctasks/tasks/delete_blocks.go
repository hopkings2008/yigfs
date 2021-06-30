package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/hopkings2008/yigfs/server/helper"
	"github.com/hopkings2008/yigfs/server/message/builder"
	"github.com/hopkings2008/yigfs/server/message/kafka/consumer"
	"github.com/hopkings2008/yigfs/server/types"
	"github.com/hopkings2008/yigfs/server/meta"
	. "github.com/hopkings2008/yigfs/server/error"
)

var (
	deleteConsumer *consumer.Consumer
	deleteBlocksQ chan []byte
	deleteFilesQ chan []byte
	ctx context.Context
	cancelFunc context.CancelFunc
	chanSize = 10000
)

type DeleteBlocksTask struct {
	GroupId string
	Topic string
}

func(d *DeleteBlocksTask) Start() (err error) {
	deleteConsumer, err = builder.CreateMessageReceiver(d.GroupId)
	if deleteConsumer.Consumer == nil || err != nil {
		helper.Logger.Error(nil, fmt.Sprintf("Failed to create deleteBlocks consumer, groupId: %v, err: %v", d.GroupId, err))
		return ErrYigFsFailedCreateMessageConsumer
	}

	err = deleteConsumer.SubscribeTopics([]string{d.Topic})
	if err != nil {
		helper.Logger.Error(nil, fmt.Sprintf("Failed to subscribe topics for delete blocks, topic: %v, err: %v", d.Topic, err))
		return err
	}

	return nil
}

func Close() {
	deleteConsumer.Consumer.Commit()
	close(deleteBlocksQ)
	close(deleteFilesQ)
}

func(d *DeleteBlocksTask) Run() {
	defer Close()
	equeue := deleteConsumer.Consumer.Events()
	deleteBlocksQ = make(chan []byte, chanSize)
	deleteFilesQ = make(chan []byte, chanSize)
	ctx, cancelFunc = context.WithCancel(context.Background())
	defer cancelFunc()
	go handleDeleteFilesTask(ctx)
	go handleDeleteBlocksTask(ctx)

	for {
		select {
		case ev := <-equeue:
			switch e := ev.(type) {
			case kafka.AssignedPartitions:
				// Assgined partition
				err := deleteConsumer.Consumer.Assign(e.Partitions)
				if err != nil {
					helper.Logger.Error(nil, fmt.Sprintf("Assgin kafka TopicPartitions failed, err: %v", err))
				}
			case kafka.RevokedPartitions:
				deleteConsumer.Consumer.Unassign()
			case *kafka.Message:
				if len(e.Value) == 0 || len(e.Key) == 0 {
					continue
				}
				key := string(e.Key)
				if key == types.DeleteBlocks {
					deleteBlocksQ <- e.Value
				} else if key == types.DeleteFile {
					deleteFilesQ <- e.Value
				} else {
					helper.Logger.Error(nil, fmt.Sprintf("The action does not supported, key: %v", key))
				}
				offset := []kafka.TopicPartition{e.TopicPartition}
				_, err := deleteConsumer.Consumer.CommitOffsets(offset)
				if err != nil {
					helper.Logger.Error(nil, fmt.Sprintf("Commit offset failed, err: %v", err))
				}
			case kafka.PartitionEOF:
				break
			case kafka.Error:
				helper.Logger.Error(nil, fmt.Sprintf("Kafka happend error, errCode: %v, errMsg: %v", e.Code(), e))
				if e.IsFatal() {
					deleteConsumer.EndQ <- true
				}
			default:
				helper.Logger.Warn(nil, fmt.Sprintf("Ignored %v\n", e))
			}
		case e := <- deleteConsumer.EndQ:
			if e {
				helper.Logger.Warn(nil, "read message from kafka goroutine exit")
				return
			}
		}
	}
}

func handleDeleteFilesTask(ctx context.Context) {
	var msg *types.DeleteFileReq
	for task := range deleteFilesQ {
		err := json.Unmarshal(task, &msg)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to parse delete file message, err: %v", err))
			continue
		}

		err = execDeleteFile(ctx, msg)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to delete the file, msg: %v", msg))
		}
	}
}

func execDeleteFile(ctx context.Context, fileReq *types.DeleteFileReq) (err error) {
	start := time.Now().UTC().UnixNano()
	// get the file's all segments
	segs, offsets, err := meta.TidbMeta.Client.GetAllExistedFileSegs(ctx, fileReq)
	if err != nil && err != ErrYigFsNoVaildSegments {
		return err
	} else if err == ErrYigFsNoVaildSegments || len(segs) == 0 {
		helper.Logger.Warn(ctx, fmt.Sprintf("The file does not have segments to deleted, region: %s, bucket: %s, ino: %d, generation: %v",
			fileReq.Region, fileReq.BucketName, fileReq.Ino, fileReq.Generation))
		return
	}

	// delete seg blocks
	err = meta.TidbMeta.Client.DeleteSegBlocks(ctx, segs)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to deleted seg blocks, region: %s, bucket: %s, ino: %d, generation: %v, err: %v",
			fileReq.Region, fileReq.BucketName, fileReq.Ino, fileReq.Generation, err))
		return
	}
	
	// delete seg info
	err = meta.TidbMeta.Client.DeleteSegInfo(ctx, fileReq, segs)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to deleted seg info, region: %s, bucket: %s, ino: %d, generation: %v, err: %v",
			fileReq.Region, fileReq.BucketName, fileReq.Ino, fileReq.Generation, err))
		return
	}
	
	// delete file blocks.
	err = meta.TidbMeta.Client.DeleteFileBlocks(ctx, fileReq, offsets)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to deleted file and seg blocks, region: %s, bucket: %s, ino: %d, generation: %v, err: %v",
			fileReq.Region, fileReq.BucketName, fileReq.Ino, fileReq.Generation, err))
		return
	}

	end := time.Now().UTC().UnixNano()
	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to exec delete the file, region: %s, bucket: %s, ino: %d, generation: %v, cost: %v", 
		fileReq.Region, fileReq.BucketName, fileReq.Ino, fileReq.Generation, end - start))
	return
}

func handleDeleteBlocksTask(ctx context.Context) {
	var msg []*types.CreateBlocksInfo
	for task := range deleteBlocksQ {
		err := json.Unmarshal(task, &msg)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to parse delete blocks message, err: %v", err))
			continue
		}

		err = execDeleteSegBlocks(ctx, msg)
		if err != nil {
			helper.Logger.Error(ctx, fmt.Sprintf("Failed to delete seg blocks, msg: %v", msg))
		}
	}
}

func removeBlocks(ctx context.Context, segs []*types.CreateBlocksInfo, blocksNum int) (err error) {
	err = meta.TidbMeta.Client.RemoveSegBlocks(ctx, segs, blocksNum)
	if err != nil {
		helper.Logger.Error(ctx, fmt.Sprintf("Failed to remove seg blocks, blocksNum: %v", blocksNum))
		return
	}
	return
}

func execDeleteSegBlocks(ctx context.Context, segs []*types.CreateBlocksInfo) (err error) {
	var currentBlocksNum int
	segsReq := make([]*types.CreateBlocksInfo, 0)
	for _, seg := range segs {
		segReq := types.CreateBlocksInfo {
			SegmentId0: seg.SegmentId0,
			SegmentId1: seg.SegmentId1,
		}

		segsReq = append(segsReq, &segReq)

		for j, block := range seg.Blocks {
			blocksNum := len(seg.Blocks)
			if blocksNum == 0 {
				helper.Logger.Warn(ctx, fmt.Sprintf("The segment does not have blocks to update, seg_id0: %v, seg_id1: %v", seg.SegmentId0, seg.SegmentId1))
				continue
			}

			segReq.Blocks = append(segReq.Blocks, block)
			currentBlocksNum++
			if currentBlocksNum == types.MaxDeleteSegBlocks {
				err = removeBlocks(ctx, segsReq, types.MaxDeleteSegBlocks)
				if err != nil {
					helper.Logger.Error(ctx, fmt.Sprintf("Failed to exec update blocks, segsNum: %v, err: %v", len(segsReq), err))
					return
				} else {
					segsReq = segsReq[:0]
					segReq.Blocks = segReq.Blocks[:0]
					currentBlocksNum = 0
					if j < blocksNum - 1 {
						segsReq = append(segsReq, &segReq)
					}
				}
			}
		}
	}

	if len(segsReq) > 0 {
		remainBlocksNum := 0
		for _, seg := range segsReq {
			remainBlocksNum += len(seg.Blocks)
		}
		err = removeBlocks(ctx, segsReq, remainBlocksNum)
		if err != nil {
			return
		}
	}

	helper.Logger.Info(ctx, fmt.Sprintf("Succeed to delete seg blocks, segsNum: %d", len(segs)))
	return
}