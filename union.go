package kapacitor

import (
	"log"
	"time"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/pipeline"
)

type UnionNode struct {
	node
	u *pipeline.UnionNode

	// Buffer of points/batches from each source.
	sources [][]timeMessage
	// the low water marks for each source.
	lowMarks []time.Time

	rename string

	legacyOuts []*LegacyEdge
}

type timeMessage interface {
	edge.Message
	edge.TimeGetter
}

// Create a new  UnionNode which combines all parent data streams into a single stream.
// No transformation of any kind is performed.
func newUnionNode(et *ExecutingTask, n *pipeline.UnionNode, l *log.Logger) (*UnionNode, error) {
	un := &UnionNode{
		u:      n,
		node:   node{Node: n, et: et, logger: l},
		rename: n.Rename,
	}
	un.node.runF = un.runUnion
	return un, nil
}

func (u *UnionNode) runUnion([]byte) error {
	// Keep buffer of values from parents so they can be ordered.

	u.sources = make([][]timeMessage, len(u.ins))
	u.lowMarks = make([]time.Time, len(u.ins))

	consumer := edge.NewMultiConsumerWithStats(u.ins, u)
	return consumer.Consume()
}

func (u *UnionNode) BufferedBatch(src int, batch edge.BufferedBatchMessage) error {
	u.timer.Start()
	defer u.timer.Stop()

	if u.rename != "" {
		batch = batch.ShallowCopy()
		batch.SetBegin(batch.Begin().ShallowCopy())
		batch.Begin().SetName(u.rename)
	}

	// Add newest point to buffer
	u.sources[src] = append(u.sources[src], batch)

	// Emit the next values
	return u.emitReady(false)
}

func (u *UnionNode) Point(src int, p edge.PointMessage) error {
	u.timer.Start()
	defer u.timer.Stop()
	if u.rename != "" {
		p = p.ShallowCopy()
		p.SetName(u.rename)
	}

	// Add newest point to buffer
	u.sources[src] = append(u.sources[src], p)

	// Emit the next values
	return u.emitReady(false)
}

func (u *UnionNode) Barrier(src int, b edge.BarrierMessage) error {
	u.timer.Start()
	defer u.timer.Stop()

	// Add newest point to buffer
	u.sources[src] = append(u.sources[src], b)

	// Emit the next values
	return u.emitReady(false)
}

func (u *UnionNode) Finish() error {
	// We are done, emit all buffered
	return u.emitReady(true)
}

func (u *UnionNode) emitReady(drain bool) error {
	emitted := true
	// Emit all points until nothing changes
	for emitted {
		emitted = false
		// Find low water mark
		var mark time.Time
		validSources := 0
		for i, values := range u.sources {
			sourceMark := u.lowMarks[i]
			if len(values) > 0 {
				t := values[0].Time()
				if mark.IsZero() || t.Before(mark) {
					mark = t
				}
				sourceMark = t
			}
			u.lowMarks[i] = sourceMark
			if !sourceMark.IsZero() {
				validSources++
				// Only consider the sourceMark if we are not draining
				if !drain && (mark.IsZero() || sourceMark.Before(mark)) {
					mark = sourceMark
				}
			}
		}
		if !drain && validSources != len(u.sources) {
			// We can't continue processing until we have
			// at least one value from each parent.
			// Unless we are draining the buffer than we can continue.
			return nil
		}

		// Emit all values that are at or below the mark.
		for i, values := range u.sources {
			var j int
			l := len(values)
			for j = 0; j < l; j++ {
				if !values[j].Time().After(mark) {
					err := u.emit(values[j])
					if err != nil {
						return err
					}
					// Note that we emitted something
					emitted = true
				} else {
					break
				}
			}
			// Drop values that were emitted
			u.sources[i] = values[j:]
		}
	}
	return nil
}

func (u *UnionNode) emit(m edge.Message) error {
	u.timer.Pause()
	defer u.timer.Resume()
	return edge.Forward(u.outs, m)
}
