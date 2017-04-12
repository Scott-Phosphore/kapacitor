package kapacitor

import (
	"errors"
	"log"
	"time"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/pipeline"
)

type ShiftNode struct {
	node
	s *pipeline.ShiftNode

	shift time.Duration
}

// Create a new  ShiftNode which shifts points and batches in time.
func newShiftNode(et *ExecutingTask, n *pipeline.ShiftNode, l *log.Logger) (*ShiftNode, error) {
	sn := &ShiftNode{
		node:  node{Node: n, et: et, logger: l},
		s:     n,
		shift: n.Shift,
	}
	sn.node.runF = sn.runShift
	if n.Shift == 0 {
		return nil, errors.New("invalid shift value: must be non zero duration")
	}
	return sn, nil
}

func (s *ShiftNode) runShift([]byte) error {
	consumer := edge.NewConsumerWithReceiver(
		s.ins[0],
		edge.NewReceiverFromForwardReceiverWithStats(
			s.outs,
			edge.NewTimedForwardReceiver(s.timer, s),
		),
	)
	return consumer.Consume()
}

func (s *ShiftNode) doShift(t edge.TimeSetter) {
	t.SetTime(t.Time().Add(s.shift))
}

func (s *ShiftNode) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	begin = begin.ShallowCopy()
	s.doShift(begin)
	return begin, nil
}

func (s *ShiftNode) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	bp = bp.ShallowCopy()
	s.doShift(bp)
	return bp, nil
}

func (s *ShiftNode) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return end, nil
}

func (s *ShiftNode) Point(p edge.PointMessage) (edge.Message, error) {
	p = p.ShallowCopy()
	s.doShift(p)
	return p, nil
}

func (s *ShiftNode) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}
