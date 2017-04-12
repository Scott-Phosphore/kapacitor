package kapacitor

import (
	"fmt"
	"log"
	"time"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
)

type stateTracker interface {
	track(t time.Time, inState bool) interface{}
	reset()
}

type stateTrackingGroup struct {
	stn *StateTrackingNode
	stateful.Expression
	stateful.ScopePool
	tracker stateTracker
}

type StateTrackingNode struct {
	node
	lambda *ast.LambdaNode
	as     string

	newTracker func() stateTracker
}

func (stn *StateTrackingNode) runStateTracking(_ []byte) error {
	consumer := edge.NewGroupedConsumer(
		stn.ins[0],
		stn,
	)
	stn.statMap.Set(statCardinalityGauge, consumer.CardinalityVar())
	return consumer.Consume()
}

func (stn *StateTrackingNode) NewGroup(group edge.GroupInfo, first edge.PointMeta) (edge.Receiver, error) {
	return edge.NewReceiverFromForwardReceiverWithStats(
		stn.outs,
		edge.NewTimedForwardReceiver(stn.timer, stn.newGroup()),
	), nil
}

func (stn *StateTrackingNode) newGroup() *stateTrackingGroup {
	// Create a new tracking group
	g := &stateTrackingGroup{
		stn: stn,
	}

	// Error is explicitly checked when the StateTrackingNode is first created.
	// TODO(nathanielc): Update the stateful expression API to be able to create a new expression from an existing expression.
	g.Expression, _ = stateful.NewExpression(stn.lambda.Expression)
	g.ScopePool = stateful.NewScopePool(ast.FindReferenceVariables(stn.lambda.Expression))

	g.tracker = stn.newTracker()
	return g
}

func (stn *StateTrackingNode) DeleteGroup(group models.GroupID) {
	// Nothing to do
}

func (g *stateTrackingGroup) BeginBatch(begin edge.BeginBatchMessage) (edge.Message, error) {
	g.tracker.reset()
	return begin, nil
}

func (g *stateTrackingGroup) BatchPoint(bp edge.BatchPointMessage) (edge.Message, error) {
	bp = bp.ShallowCopy()
	err := g.track(bp)
	if err != nil {
		g.stn.incrementErrorCount()
		g.stn.logger.Println("E! error while evaluating expression:", err)
		return nil, nil
	}
	return bp, nil
}

func (g *stateTrackingGroup) EndBatch(end edge.EndBatchMessage) (edge.Message, error) {
	return end, nil
}

func (g *stateTrackingGroup) Point(p edge.PointMessage) (edge.Message, error) {
	p = p.ShallowCopy()
	err := g.track(p)
	if err != nil {
		g.stn.incrementErrorCount()
		g.stn.logger.Println("E! error while evaluating expression:", err)
		return nil, nil
	}
	return p, nil
}

func (g *stateTrackingGroup) track(p edge.FieldsTagsTimeSetter) error {
	pass, err := EvalPredicate(g.Expression, g.ScopePool, p)
	if err != nil {
		return err
	}

	fields := p.Fields().Copy()
	fields[g.stn.as] = g.tracker.track(p.Time(), pass)
	p.SetFields(fields)
	return nil
}

func (g *stateTrackingGroup) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}

type stateDurationTracker struct {
	sd *pipeline.StateDurationNode

	startTime time.Time
}

func (sdt *stateDurationTracker) reset() {
	sdt.startTime = time.Time{}
}

func (sdt *stateDurationTracker) track(t time.Time, inState bool) interface{} {
	if !inState {
		sdt.startTime = time.Time{}
		return float64(-1)
	}

	if sdt.startTime.IsZero() {
		sdt.startTime = t
	}
	return float64(t.Sub(sdt.startTime)) / float64(sdt.sd.Unit)
}

func newStateDurationNode(et *ExecutingTask, sd *pipeline.StateDurationNode, l *log.Logger) (*StateTrackingNode, error) {
	if sd.Lambda == nil {
		return nil, fmt.Errorf("nil expression passed to StateDurationNode")
	}
	// Validate lambda expression
	if _, err := stateful.NewExpression(sd.Lambda.Expression); err != nil {
		return nil, err
	}
	stn := &StateTrackingNode{
		node:       node{Node: sd, et: et, logger: l},
		lambda:     sd.Lambda,
		as:         sd.As,
		newTracker: func() stateTracker { return &stateDurationTracker{sd: sd} },
	}
	stn.node.runF = stn.runStateTracking
	return stn, nil
}

type stateCountTracker struct {
	count int64
}

func (sct *stateCountTracker) reset() {
	sct.count = 0
}

func (sct *stateCountTracker) track(t time.Time, inState bool) interface{} {
	if !inState {
		sct.count = 0
		return int64(-1)
	}

	sct.count++
	return sct.count
}

func newStateCountNode(et *ExecutingTask, sc *pipeline.StateCountNode, l *log.Logger) (*StateTrackingNode, error) {
	if sc.Lambda == nil {
		return nil, fmt.Errorf("nil expression passed to StateCountNode")
	}
	// Validate lambda expression
	if _, err := stateful.NewExpression(sc.Lambda.Expression); err != nil {
		return nil, err
	}
	stn := &StateTrackingNode{
		node:       node{Node: sc, et: et, logger: l},
		lambda:     sc.Lambda,
		as:         sc.As,
		newTracker: func() stateTracker { return &stateCountTracker{} },
	}
	stn.node.runF = stn.runStateTracking
	return stn, nil
}
