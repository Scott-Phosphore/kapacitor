package kapacitor

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/wlog"
)

type LogNode struct {
	node

	key string
	buf bytes.Buffer
	enc *json.Encoder

	batchBuffer *edge.BatchBuffer
}

// Create a new  LogNode which logs all data it receives
func newLogNode(et *ExecutingTask, n *pipeline.LogNode, l *log.Logger) (*LogNode, error) {
	level, ok := wlog.StringToLevel[strings.ToUpper(n.Level)]
	if !ok {
		return nil, fmt.Errorf("invalid log level %s", n.Level)
	}
	nn := &LogNode{
		node:        node{Node: n, et: et, logger: l},
		key:         fmt.Sprintf("%c! %s", wlog.ReverseLevels[level], n.Prefix),
		batchBuffer: new(edge.BatchBuffer),
	}
	nn.enc = json.NewEncoder(&nn.buf)
	nn.node.runF = nn.runLog
	return nn, nil
}

func (s *LogNode) runLog([]byte) error {
	consumer := edge.NewConsumerWithReceiver(
		s.ins[0],
		edge.NewReceiverFromForwardReceiverWithStats(
			s.outs,
			edge.NewTimedForwardReceiver(s.timer, s),
		),
	)
	return consumer.Consume()

}

func (s *LogNode) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	return nil, s.batchBuffer.BeginBatch(begin)
}

func (s *LogNode) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	return nil, s.batchBuffer.BatchPoint(bp)
}

func (s *LogNode) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return s.BufferedBatch(s.batchBuffer.BufferedBatchMessage(end))
}

func (s *LogNode) BufferedBatch(batch edge.BufferedBatchMessage) (edge.Message, error) {
	s.buf.Reset()
	if err := s.enc.Encode(batch); err != nil {
		s.incrementErrorCount()
		s.logger.Println("E!", err)
		return batch, nil
	}
	s.logger.Println(s.key, s.buf.String())
	return batch, nil
}

func (s *LogNode) Point(p edge.PointMessage) (edge.Message, error) {
	s.buf.Reset()
	if err := s.enc.Encode(p); err != nil {
		s.incrementErrorCount()
		s.logger.Println("E!", err)
		return p, nil
	}
	s.logger.Println(s.key, s.buf.String())
	return p, nil
}

func (s *LogNode) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}
