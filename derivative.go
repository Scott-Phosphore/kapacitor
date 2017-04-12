package kapacitor

import (
	"log"
	"time"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
)

type DerivativeNode struct {
	node
	d *pipeline.DerivativeNode
}

// Create a new derivative node.
func newDerivativeNode(et *ExecutingTask, n *pipeline.DerivativeNode, l *log.Logger) (*DerivativeNode, error) {
	dn := &DerivativeNode{
		node: node{Node: n, et: et, logger: l},
		d:    n,
	}
	// Create stateful expressions
	dn.node.runF = dn.runDerivative
	return dn, nil
}

func (d *DerivativeNode) runDerivative([]byte) error {
	consumer := edge.NewGroupedConsumer(
		d.ins[0],
		d,
	)
	d.statMap.Set(statCardinalityGauge, consumer.CardinalityVar())
	return consumer.Consume()
}

func (d *DerivativeNode) NewGroup(group edge.GroupInfo, first edge.PointMeta) (edge.Receiver, error) {
	return edge.NewReceiverFromForwardReceiverWithStats(
		d.outs,
		edge.NewTimedForwardReceiver(d.timer, d.newGroup()),
	), nil
}

func (d *DerivativeNode) newGroup() *derivativeGroup {
	return &derivativeGroup{
		n: d,
	}
}

func (d *DerivativeNode) DeleteGroup(group models.GroupID) {
}

type derivativeGroup struct {
	n        *DerivativeNode
	previous edge.FieldsTagsTimeGetter
}

func (g *derivativeGroup) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	if s := begin.SizeHint(); s > 0 {
		begin = begin.ShallowCopy()
		begin.SetSizeHint(s - 1)
	}
	g.previous = nil
	return begin, nil
}

func (g *derivativeGroup) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	np := bp.ShallowCopy()
	emit := g.doDerivative(bp, np)
	if emit {
		return np, nil
	}
	return nil, nil
}

func (g *derivativeGroup) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return end, nil
}

func (g *derivativeGroup) Point(p edge.PointMessage) (edge.Message, error) {
	np := p.ShallowCopy()
	emit := g.doDerivative(p, np)
	if emit {
		return np, nil
	}
	return nil, nil
}

// doDerivative computes the derivative with respect to g.previous and p.
// The resulting derivative value will be set on n.
func (g *derivativeGroup) doDerivative(p edge.FieldsTagsTimeGetter, n edge.FieldsTagsTimeSetter) bool {
	var prevFields, currFields models.Fields
	var prevTime, currTime time.Time
	if g.previous != nil {
		prevFields = g.previous.Fields()
		prevTime = g.previous.Time()
	}
	currFields = p.Fields()
	currTime = p.Time()
	value, store, emit := g.n.derivative(
		prevFields, currFields,
		prevTime, currTime,
	)
	if store {
		g.previous = p
	}
	if !emit {
		return false
	}

	fields := n.Fields().Copy()
	fields[g.n.d.As] = value
	n.SetFields(fields)
	return true
}

func (g *derivativeGroup) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}

// derivative calculates the derivative between prev and cur.
// Return is the resulting derivative, whether the current point should be
// stored as previous, and whether the point result should be emitted.
func (d *DerivativeNode) derivative(prev, curr models.Fields, prevTime, currTime time.Time) (float64, bool, bool) {
	f1, ok := numToFloat(curr[d.d.Field])
	if !ok {
		d.incrementErrorCount()
		d.logger.Printf("E! cannot apply derivative to type %T", curr[d.d.Field])
		return 0, false, false
	}

	f0, ok := numToFloat(prev[d.d.Field])
	if !ok {
		// The only time this will fail to parse is if there is no previous.
		// Because we only return `store=true` if current parses successfully, we will
		// never get a previous which doesn't parse.
		return 0, true, false
	}

	elapsed := float64(currTime.Sub(prevTime))
	if elapsed == 0 {
		d.incrementErrorCount()
		d.logger.Printf("E! cannot perform derivative elapsed time was 0")
		return 0, true, false
	}
	diff := f1 - f0
	// Drop negative values for non-negative derivatives
	if d.d.NonNegativeFlag && diff < 0 {
		return 0, true, false
	}

	value := float64(diff) / (elapsed / float64(d.d.Unit))
	return value, true, true
}

func numToFloat(num interface{}) (float64, bool) {
	switch n := num.(type) {
	case int64:
		return float64(n), true
	case float64:
		return n, true
	default:
		return 0, false
	}
}
