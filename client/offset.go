package client

import (
	"fmt"

	"github.com/pjvds/edgy/api"
)

type Offset struct {
	MessageId uint64 `json:"message_id"`
	Position  int64  `json:"position"`
	SegmentId uint64 `json:"segment_id"`
}

var OffsetBeginning = NewOffset(0, 0, 0)

func NewOffset(messageId uint64, position int64, segmentId uint64) Offset {
	return Offset{
		MessageId: messageId,
		Position:  position,
		SegmentId: segmentId,
	}
}

func (this Offset) String() string {
	return fmt.Sprintf("%v@%v/%v", this.MessageId, this.SegmentId, this.Position)
}

func (this Offset) toOffsetData() *api.OffsetData {
	return &api.OffsetData{
		MessageId:   this.MessageId,
		EndPosition: this.Position,
		SegmentId:   this.SegmentId,
	}
}

func offsetFromOffsetData(offsetData *api.OffsetData) Offset {
	return Offset{
		MessageId: offsetData.MessageId,
		Position:  offsetData.EndPosition,
		SegmentId: offsetData.SegmentId,
	}
}
