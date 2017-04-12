package kapacitor

import (
	"log"
	"sort"
	"sync"
	"time"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
)

type GroupByNode struct {
	node
	g *pipeline.GroupByNode

	byName   bool
	tagNames []string

	begin      edge.BeginBatchMessage
	dimensions models.Dimensions

	allDimensions bool

	mu       sync.RWMutex
	lastTime time.Time
	groups   map[models.GroupID]edge.BufferedBatchMessage
}

// Create a new GroupByNode which splits the stream dynamically based on the specified dimensions.
func newGroupByNode(et *ExecutingTask, n *pipeline.GroupByNode, l *log.Logger) (*GroupByNode, error) {
	gn := &GroupByNode{
		node:   node{Node: n, et: et, logger: l},
		g:      n,
		groups: make(map[models.GroupID]edge.BufferedBatchMessage),
	}
	gn.node.runF = gn.runGroupBy

	gn.allDimensions, gn.tagNames = determineTagNames(n.Dimensions, n.ExcludedDimensions)
	gn.byName = n.ByMeasurementFlag
	return gn, nil
}

func (g *GroupByNode) runGroupBy([]byte) error {
	valueF := func() int64 {
		g.mu.RLock()
		l := len(g.groups)
		g.mu.RUnlock()
		return int64(l)
	}
	g.statMap.Set(statCardinalityGauge, expvar.NewIntFuncGauge(valueF))

	consumer := edge.NewConsumerWithReceiver(
		g.ins[0],
		g,
	)
	return consumer.Consume()
}

func (g *GroupByNode) Point(p edge.PointMessage) error {
	p = p.ShallowCopy()
	g.timer.Start()
	dims := p.Dimensions()
	dims.ByName = dims.ByName || g.byName
	dims.TagNames = computeTagNames(p.Tags(), g.allDimensions, g.tagNames, g.g.ExcludedDimensions)
	p.SetDimensions(dims)
	g.timer.Stop()
	if err := edge.Forward(g.outs, p); err != nil {
		return err
	}
	return nil
}

func (g *GroupByNode) BeginBatch(begin edge.BeginBatchMessage) error {
	g.timer.Start()
	defer g.timer.Stop()

	g.emit(begin.Time())

	g.begin = begin
	g.dimensions = begin.Dimensions()
	g.dimensions.ByName = g.dimensions.ByName || g.byName

	return nil
}

func (g *GroupByNode) BatchPoint(bp edge.BatchPointMessage) error {
	g.timer.Start()
	defer g.timer.Stop()

	g.dimensions.TagNames = computeTagNames(bp.Tags(), g.allDimensions, g.tagNames, g.g.ExcludedDimensions)
	groupID := models.ToGroupID(g.begin.Name(), bp.Tags(), g.dimensions)
	group, ok := g.groups[groupID]
	if !ok {
		// Create new begin message
		newBegin := g.begin.ShallowCopy()
		newBegin.SetTagsAndDimensions(bp.Tags(), g.dimensions)

		// Create buffer for group batch
		group = edge.NewBufferedBatchMessage(
			newBegin,
			make([]edge.BatchPointMessage, 0, newBegin.SizeHint()),
			edge.NewEndBatchMessage(),
		)
		g.mu.Lock()
		g.groups[groupID] = group
		g.mu.Unlock()
	}
	group.SetPoints(append(group.Points(), bp))

	return nil
}

func (g *GroupByNode) EndBatch(end edge.EndBatchMessage) error {
	return nil
}

func (g *GroupByNode) Barrier(b edge.BarrierMessage) error {
	g.timer.Start()
	err := g.emit(b.Time())
	g.timer.Stop()
	return err
}

// emit sends all groups before time t to children nodes.
// The node timer must be started when calling this method.
func (g *GroupByNode) emit(t time.Time) error {
	// TODO: ensure this time comparison works with barrier messages
	if !t.Equal(g.lastTime) {
		g.lastTime = t
		// Emit all groups
		for id, group := range g.groups {
			// Update SizeHint since we know the final point count
			group.Begin().SetSizeHint(len(group.Points()))
			// Sort points since we didn't guarantee insertion order was sorted
			sort.Sort(edge.BatchPointMessages(group.Points()))
			// Send group batch to all children
			g.timer.Pause()
			if err := edge.Forward(g.outs, group); err != nil {
				return err
			}
			g.timer.Resume()
			g.mu.Lock()
			// Remove from group
			delete(g.groups, id)
			g.mu.Unlock()
		}
	}
	return nil
}

func determineTagNames(dimensions []interface{}, excluded []string) (allDimensions bool, realDimensions []string) {
	for _, dim := range dimensions {
		switch d := dim.(type) {
		case string:
			realDimensions = append(realDimensions, d)
		case *ast.StarNode:
			allDimensions = true
		}
	}
	sort.Strings(realDimensions)
	realDimensions = filterExcludedTagNames(realDimensions, excluded)
	return
}

func filterExcludedTagNames(tagNames, excluded []string) []string {
	filtered := tagNames[0:0]
	for _, t := range tagNames {
		found := false
		for _, x := range excluded {
			if x == t {
				found = true
				break
			}
		}
		if !found {
			filtered = append(filtered, t)
		}
	}
	return filtered
}

func computeTagNames(tags models.Tags, allDimensions bool, tagNames, excluded []string) []string {
	if allDimensions {
		return filterExcludedTagNames(models.SortedKeys(tags), excluded)
	}
	return tagNames
}
