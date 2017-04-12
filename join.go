package kapacitor

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/expvar"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/pkg/errors"
)

type JoinNode struct {
	node
	j         *pipeline.JoinNode
	fill      influxql.FillOption
	fillValue interface{}

	groupsMu sync.RWMutex
	groups   map[models.GroupID]*joinGroup

	// Represents the lower bound of times per group per source
	lowMarks map[srcGroup]time.Time

	// Buffer for caching points that need to be matched with specific points.
	matchGroupsBuffer map[models.GroupID][]srcPoint
	// Buffer for caching specific points until their match arrivces.
	specificGroupsBuffer map[models.GroupID][]srcPoint

	reported    map[int]bool
	allReported bool
}

// Create a new JoinNode, which takes pairs from parent streams combines them into a single point.
func newJoinNode(et *ExecutingTask, n *pipeline.JoinNode, l *log.Logger) (*JoinNode, error) {
	jn := &JoinNode{
		j:                    n,
		node:                 node{Node: n, et: et, logger: l},
		groups:               make(map[models.GroupID]*joinGroup),
		matchGroupsBuffer:    make(map[models.GroupID][]srcPoint),
		specificGroupsBuffer: make(map[models.GroupID][]srcPoint),
		lowMarks:             make(map[srcGroup]time.Time),
		reported:             make(map[int]bool),
	}
	// Set fill
	switch fill := n.Fill.(type) {
	case string:
		switch fill {
		case "null":
			jn.fill = influxql.NullFill
		case "none":
			jn.fill = influxql.NoFill
		default:
			return nil, fmt.Errorf("unexpected fill option %s", fill)
		}
	case int64, float64:
		jn.fill = influxql.NumberFill
		jn.fillValue = fill
	default:
		jn.fill = influxql.NoFill
	}
	jn.node.runF = jn.runJoin
	return jn, nil
}

func (j *JoinNode) runJoin([]byte) error {
	consumer := edge.NewMultiConsumerWithStats(j.ins, j)
	valueF := func() int64 {
		j.groupsMu.RLock()
		l := len(j.groups)
		j.groupsMu.RUnlock()
		return int64(l)
	}
	j.statMap.Set(statCardinalityGauge, expvar.NewIntFuncGauge(valueF))

	return consumer.Consume()
}

func (j *JoinNode) BufferedBatch(src int, batch edge.BufferedBatchMessage) error {
	return j.doMessage(src, batch)
}

func (j *JoinNode) Point(src int, p edge.PointMessage) error {
	return j.doMessage(src, p)
}

func (j *JoinNode) Barrier(src int, b edge.BarrierMessage) error {
	//TODO: implement barrier
	return nil
}

func (j *JoinNode) Finish() error {
	// No more points are coming signal all groups to finish up.
	for _, group := range j.groups {
		if err := group.Finish(); err != nil {
			return err
		}
	}
	return nil
}

type messageMeta interface {
	edge.Message
	edge.PointMeta
}
type srcPoint struct {
	Src int
	Msg messageMeta
}

func (j *JoinNode) doMessage(src int, m messageMeta) error {
	j.timer.Start()
	defer j.timer.Stop()
	if len(j.j.Dimensions) > 0 {
		// Match points with their group based on join dimensions.
		j.matchPoints(srcPoint{Src: src, Msg: m})
	} else {
		// Just send point on to group, we are not joining on specific dimensions.
		group := j.getOrCreateGroup(m.GroupID())
		group.Collect(src, m)
	}
	return nil
}

// The purpose of this method is to match more specific points
// with the less specific points as they arrive.
//
// Where 'more specific' means, that a point has more dimensions than the join.on dimensions.
func (j *JoinNode) matchPoints(p srcPoint) {
	// Specific points may be sent to the joinset without a matching point, but not the other way around.
	// This is because the specific points have the needed specific tag data.
	// The joinset will later handle the fill inner/outer join operations.

	if !j.allReported {
		j.reported[p.Src] = true
		j.allReported = len(j.reported) == len(j.ins)
	}
	t := p.Msg.Time().Round(j.j.Tolerance)

	groupId := models.ToGroupID(
		p.Msg.Name(),
		p.Msg.GroupInfo().Tags,
		models.Dimensions{
			ByName:   p.Msg.Dimensions().ByName,
			TagNames: j.j.Dimensions,
		},
	)
	// Update current srcGroup lowMark
	srcG := srcGroup{src: p.Src, groupId: groupId}
	j.lowMarks[srcG] = t

	// Determine lowMark, the oldest time per parent per group.
	var lowMark time.Time
	if j.allReported {
		for s := 0; s < len(j.ins); s++ {
			sg := srcGroup{src: s, groupId: groupId}
			if lm := j.lowMarks[sg]; lowMark.IsZero() || lm.Before(lowMark) {
				lowMark = lm
			}
		}
	}

	// Check for cached specific points that can now be sent alone.
	if j.allReported {
		// Send all cached specific point that won't match anymore.
		var i int
		buf := j.specificGroupsBuffer[groupId]
		l := len(buf)
		for i = 0; i < l; i++ {
			st := buf[i].Msg.Time().Round(j.j.Tolerance)
			if st.Before(lowMark) {
				// Send point by itself since it won't get a match.
				j.sendSpecificPoint(buf[i])
			} else {
				break
			}
		}
		// Remove all sent points.
		j.specificGroupsBuffer[groupId] = buf[i:]
	}

	if len(p.Msg.Dimensions().TagNames) > len(j.j.Dimensions) {
		// We have a specific point and three options:
		// 1. Find the cached match point and send both to group.
		// 2. Cache the specific point for later.
		// 3. Send the specific point alone if it is no longer possible that a match will arrive.

		// Search for a match.
		// Also purge any old match points.
		matches := j.matchGroupsBuffer[groupId]
		matched := false
		var i int
		l := len(matches)
		for i = 0; i < l; i++ {
			match := matches[i]
			pt := match.Msg.Time().Round(j.j.Tolerance)
			if pt.Equal(t) {
				// Option 1, send both points
				j.sendMatchPoint(p, match)
				matched = true
			}
			if !pt.Before(lowMark) {
				break
			}
		}
		if j.allReported {
			// Can't trust lowMark until all parents have reported.
			// Remove any unneeded match points.
			j.matchGroupsBuffer[groupId] = matches[i:]
		}

		// If the point didn't match that leaves us with options 2 and 3.
		if !matched {
			if j.allReported && t.Before(lowMark) {
				// Option 3
				// Send this specific point by itself since it won't get a match.
				j.sendSpecificPoint(p)
			} else {
				// Option 2
				// Cache this point for when its match arrives.
				j.specificGroupsBuffer[groupId] = append(j.specificGroupsBuffer[groupId], p)
			}
		}
	} else {
		// Cache match point.
		j.matchGroupsBuffer[groupId] = append(j.matchGroupsBuffer[groupId], p)

		// Send all specific points that match, to the group.
		var i int
		buf := j.specificGroupsBuffer[groupId]
		l := len(buf)
		for i = 0; i < l; i++ {
			st := buf[i].Msg.Time().Round(j.j.Tolerance)
			if st.Equal(t) {
				j.sendMatchPoint(buf[i], p)
			} else {
				break
			}
		}
		// Remove all sent points
		j.specificGroupsBuffer[groupId] = buf[i:]
	}
}

// Add the specific tags from the specific point to the matched point
// and then send both on to the group.
func (j *JoinNode) sendMatchPoint(specific, matched srcPoint) {
	var newMatched messageMeta
	switch msg := matched.Msg.(type) {
	case edge.BufferedBatchMessage:
		b := msg.ShallowCopy()
		b.SetBegin(b.Begin().ShallowCopy())
		b.Begin().SetTags(specific.Msg.GroupInfo().Tags)
		newMatched = b
	case edge.PointMessage:
		p := msg.ShallowCopy()
		info := specific.Msg.GroupInfo()
		p.SetTagsAndDimensions(info.Tags, info.Dimensions)
		newMatched = p
	}
	group := j.getOrCreateGroup(specific.Msg.GroupID())
	// Collect specific point
	group.Collect(specific.Src, specific.Msg)
	// Collect new matched point
	group.Collect(matched.Src, newMatched)
}

// Send only the specific point to the group
func (j *JoinNode) sendSpecificPoint(specific srcPoint) {
	group := j.getOrCreateGroup(specific.Msg.GroupID())
	group.Collect(specific.Src, specific.Msg)
}

// safely get the group for the point or create one if it doesn't exist.
func (j *JoinNode) getOrCreateGroup(groupID models.GroupID) *joinGroup {
	group := j.groups[groupID]
	if group == nil {
		group = j.newGroup(len(j.ins))
		j.groupsMu.Lock()
		j.groups[groupID] = group
		j.groupsMu.Unlock()
	}
	return group
}

func (j *JoinNode) newGroup(n int) *joinGroup {
	return &joinGroup{
		j:    j,
		sets: make(map[time.Time][]*joinset),
		head: make([]time.Time, n),
	}
}

// handles emitting joined sets once enough data has arrived from parents.
type joinGroup struct {
	j *JoinNode

	sets       map[time.Time][]*joinset
	head       []time.Time
	oldestTime time.Time
}

func (g *joinGroup) Finish() error {
	return g.emitAll()
}

// Collect a point from a given parent.
// emit the oldest set if we have collected enough data.
func (g *joinGroup) Collect(src int, p timeMessage) error {
	t := p.Time().Round(g.j.j.Tolerance)
	if t.Before(g.oldestTime) || g.oldestTime.IsZero() {
		g.oldestTime = t
	}

	var set *joinset
	sets := g.sets[t]
	if len(sets) == 0 {
		set = g.newJoinset(t)
		sets = append(sets, set)
		g.sets[t] = sets
	}
	for i := 0; i < len(sets); i++ {
		if !sets[i].Has(src) {
			set = sets[i]
			break
		}
	}
	if set == nil {
		set = g.newJoinset(t)
		sets = append(sets, set)
		g.sets[t] = sets
	}
	set.Set(src, p)

	// Update head
	g.head[src] = t

	onlyReadySets := false
	for _, t := range g.head {
		if !t.After(g.oldestTime) {
			onlyReadySets = true
			break
		}
	}
	err := g.emit(onlyReadySets)
	if err != nil {
		return err
	}
	return nil
}

func (g *joinGroup) newJoinset(t time.Time) *joinset {
	return newJoinset(
		g.j,
		g.j.j.StreamName,
		g.j.fill,
		g.j.fillValue,
		g.j.j.Names,
		g.j.j.Delimiter,
		g.j.j.Tolerance,
		t,
		g.j.logger,
	)
}

// emit a set and update the oldestTime.
func (g *joinGroup) emit(onlyReadySets bool) error {
	sets := g.sets[g.oldestTime]
	i := 0
	for ; i < len(sets); i++ {
		if sets[i].Ready() || !onlyReadySets {
			err := g.emitJoinedSet(sets[i])
			if err != nil {
				return err
			}
		} else {
			break
		}
	}
	if i == len(sets) {
		delete(g.sets, g.oldestTime)
	} else {
		g.sets[g.oldestTime] = sets[i:]
	}

	g.oldestTime = time.Time{}
	for t := range g.sets {
		if g.oldestTime.IsZero() || t.Before(g.oldestTime) {
			g.oldestTime = t
		}
	}
	return nil
}

// emit sets until we have none left.
func (g *joinGroup) emitAll() error {
	var lastErr error
	for len(g.sets) > 0 {
		err := g.emit(false)
		if err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// emit a single joined set
func (g *joinGroup) emitJoinedSet(set *joinset) error {
	if set.name == "" {
		set.name = set.First().(edge.NameGetter).Name()
	}
	switch g.j.Wants() {
	case pipeline.StreamEdge:
		p, err := set.JoinIntoPoint()
		if err != nil {
			return errors.Wrap(err, "failed to join into point")
		}
		if p != nil {
			if err := edge.Forward(g.j.outs, p); err != nil {
				return err
			}
		}
	case pipeline.BatchEdge:
		b, err := set.JoinIntoBatch()
		if err != nil {
			return errors.Wrap(err, "failed to join into batch")
		}
		if b != nil {
			if err := edge.Forward(g.j.outs, b); err != nil {
				return err
			}
		}
	}
	return nil
}

// A groupId and its parent
type srcGroup struct {
	src     int
	groupId models.GroupID
}

// represents a set of points or batches from the same joined time
type joinset struct {
	j         *JoinNode
	name      string
	fill      influxql.FillOption
	fillValue interface{}
	prefixes  []string
	delimiter string

	time      time.Time
	tolerance time.Duration
	values    []edge.Message

	expected int
	size     int
	finished int

	first int

	logger *log.Logger
}

func newJoinset(
	n *JoinNode,
	name string,
	fill influxql.FillOption,
	fillValue interface{},
	prefixes []string,
	delimiter string,
	tolerance time.Duration,
	time time.Time,
	l *log.Logger,
) *joinset {
	expected := len(prefixes)
	return &joinset{
		j:         n,
		name:      name,
		fill:      fill,
		fillValue: fillValue,
		prefixes:  prefixes,
		delimiter: delimiter,
		expected:  expected,
		values:    make([]edge.Message, expected),
		first:     expected,
		time:      time,
		tolerance: tolerance,
		logger:    l,
	}
}

func (js *joinset) Ready() bool {
	return js.size == js.expected
}

func (js *joinset) Has(i int) bool {
	return js.values[i] != nil
}

// add a point to the set from a given parent index.
func (js *joinset) Set(i int, v edge.Message) {
	if i < js.first {
		js.first = i
	}
	js.values[i] = v
	js.size++
}

// a valid point in the set
func (js *joinset) First() edge.Message {
	return js.values[js.first]
}

// join all points into a single point
func (js *joinset) JoinIntoPoint() (edge.PointMessage, error) {
	first, ok := js.First().(edge.PointMessage)
	if !ok {
		return nil, fmt.Errorf("unexpected type of first value %T", js.First())
	}
	firstFields := first.Fields()
	fields := make(models.Fields, js.size*len(firstFields))
	for i, v := range js.values {
		if v == nil {
			switch js.fill {
			case influxql.NullFill:
				for k := range firstFields {
					fields[js.prefixes[i]+js.delimiter+k] = nil
				}
			case influxql.NumberFill:
				for k := range firstFields {
					fields[js.prefixes[i]+js.delimiter+k] = js.fillValue
				}
			default:
				// inner join no valid point possible
				return nil, nil
			}
		} else {
			p, ok := v.(edge.FieldGetter)
			if !ok {
				return nil, fmt.Errorf("unexpected type %T", v)
			}
			for k, v := range p.Fields() {
				fields[js.prefixes[i]+js.delimiter+k] = v
			}
		}
	}
	np := edge.NewPointMessage(
		js.name, "", "",
		first.Dimensions(),
		fields,
		first.GroupInfo().Tags,
		js.time,
	)
	return np, nil
}

// join all batches the set into a single batch
func (js *joinset) JoinIntoBatch() (edge.BufferedBatchMessage, error) {
	first, ok := js.First().(edge.BufferedBatchMessage)
	if !ok {
		return nil, fmt.Errorf("unexpected type of first value %T", js.First())
	}
	newBegin := edge.NewBeginBatchMessage(
		js.name,
		first.Tags(),
		first.Dimensions().ByName,
		js.time,
		0,
	)
	newPoints := make([]edge.BatchPointMessage, 0, len(first.Points()))
	empty := make([]bool, js.expected)
	emptyCount := 0
	indexes := make([]int, js.expected)
	var fieldNames []string

BATCH_POINT:
	for emptyCount < js.expected {
		set := make([]edge.BatchPointMessage, js.expected)
		setTime := time.Time{}
		count := 0
		for i, batch := range js.values {
			if empty[i] {
				continue
			}
			if batch == nil {
				emptyCount++
				empty[i] = true
				continue
			}
			b, ok := batch.(edge.BufferedBatchMessage)
			if !ok {
				return nil, fmt.Errorf("unexpected type of batch value %T", batch)
			}
			if indexes[i] == len(b.Points()) {
				emptyCount++
				empty[i] = true
				continue
			}
			bp := b.Points()[indexes[i]]
			t := bp.Time().Round(js.tolerance)
			if setTime.IsZero() {
				setTime = t
			}
			if t.Before(setTime) {
				// We need to backup
				setTime = t
				for j := range set {
					if set[j] != nil {
						indexes[j]--
					}
					set[j] = nil
				}
				set[i] = bp
				indexes[i]++
				count = 1
			} else if t.Equal(setTime) {
				if fieldNames == nil {
					for k := range bp.Fields() {
						fieldNames = append(fieldNames, k)
					}
				}
				set[i] = bp
				indexes[i]++
				count++
			}
		}
		// we didn't get any points from any group we must be empty
		// skip this set
		if count == 0 {
			continue
		}
		// Join all batch points in set
		fields := make(models.Fields, js.expected*len(fieldNames))
		for i, bp := range set {
			if bp == nil {
				switch js.fill {
				case influxql.NullFill:
					for _, k := range fieldNames {
						fields[js.prefixes[i]+js.delimiter+k] = nil
					}
				case influxql.NumberFill:
					for _, k := range fieldNames {
						fields[js.prefixes[i]+js.delimiter+k] = js.fillValue
					}
				default:
					// inner join no valid point possible
					continue BATCH_POINT
				}
			} else {
				for k, v := range bp.Fields() {
					fields[js.prefixes[i]+js.delimiter+k] = v
				}
			}
		}
		bp := edge.NewBatchPointMessage(
			fields,
			newBegin.Tags(),
			setTime,
		)
		newPoints = append(newPoints, bp)
	}
	newBegin.SetSizeHint(len(newPoints))
	return edge.NewBufferedBatchMessage(
		newBegin,
		newPoints,
		edge.NewEndBatchMessage(),
	), nil
}

type durationVar struct {
	expvar.Int
}

func (d *durationVar) String() string {
	return time.Duration(d.IntValue()).String()
}
