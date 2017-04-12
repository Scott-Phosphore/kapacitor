package kapacitor

import (
	"fmt"
	"log"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

const (
	statsKapacitorLoopbackPointsWritten = "points_written"
)

type KapacitorLoopbackNode struct {
	node
	k *pipeline.KapacitorLoopbackNode

	pointsWritten *expvar.Int

	begin edge.BeginBatchMessage
}

func newKapacitorLoopbackNode(et *ExecutingTask, n *pipeline.KapacitorLoopbackNode, l *log.Logger) (*KapacitorLoopbackNode, error) {
	kn := &KapacitorLoopbackNode{
		node: node{Node: n, et: et, logger: l},
		k:    n,
	}
	kn.node.runF = kn.runOut
	// Check that a loop has not been created within this task
	for _, dbrp := range et.Task.DBRPs {
		if dbrp.Database == n.Database && dbrp.RetentionPolicy == n.RetentionPolicy {
			return nil, fmt.Errorf("loop detected on dbrp: %v", dbrp)
		}
	}
	return kn, nil
}

func (k *KapacitorLoopbackNode) runOut([]byte) error {
	k.pointsWritten = &expvar.Int{}
	k.statMap.Set(statsInfluxDBPointsWritten, k.pointsWritten)

	consumer := edge.NewConsumerWithReceiver(
		k.ins[0],
		k,
	)
	return consumer.Consume()
}

func (k *KapacitorLoopbackNode) Point(p edge.PointMessage) error {
	k.timer.Start()
	defer k.timer.Stop()

	p = p.ShallowCopy()

	if k.k.Database != "" {
		p.SetDatabase(k.k.Database)
	}
	if k.k.RetentionPolicy != "" {
		p.SetRetentionPolicy(k.k.RetentionPolicy)
	}
	if k.k.Measurement != "" {
		p.SetName(k.k.Measurement)
	}
	if len(k.k.Tags) > 0 {
		tags := p.Tags().Copy()
		for k, v := range k.k.Tags {
			tags[k] = v
		}
		p.SetTags(tags)
	}

	k.timer.Pause()
	err := k.et.tm.WriteKapacitorPoint(p)
	k.timer.Resume()

	if err != nil {
		k.incrementErrorCount()
		k.logger.Println("E! failed to write point over loopback")
	} else {
		k.pointsWritten.Add(1)
	}
	return nil
}

func (k *KapacitorLoopbackNode) BeginBatch(begin edge.BeginBatchMessage) error {
	k.begin = begin
	return nil
}

func (k *KapacitorLoopbackNode) BatchPoint(bp edge.BatchPointMessage) error {
	tags := bp.Tags()
	if len(k.k.Tags) > 0 {
		tags = bp.Tags().Copy()
		for k, v := range k.k.Tags {
			tags[k] = v
		}
	}
	p := edge.NewPointMessage(
		k.begin.Name(),
		k.k.Database,
		k.k.RetentionPolicy,
		models.Dimensions{},
		bp.Fields(),
		tags,
		bp.Time(),
	)

	k.timer.Pause()
	err := k.et.tm.WriteKapacitorPoint(p)
	k.timer.Resume()

	if err != nil {
		k.incrementErrorCount()
		k.logger.Println("E! failed to write point over loopback")
	} else {
		k.pointsWritten.Add(1)
	}
	return nil
}
func (k *KapacitorLoopbackNode) EndBatch(edge.EndBatchMessage) error {
	return nil
}
func (k *KapacitorLoopbackNode) Barrier(edge.BarrierMessage) error {
	return nil
}
