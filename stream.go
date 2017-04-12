package kapacitor

import (
	"errors"
	"fmt"
	"log"

	"github.com/influxdata/kapacitor/edge"
	"github.com/influxdata/kapacitor/models"
	"github.com/influxdata/kapacitor/pipeline"
	"github.com/influxdata/kapacitor/tick/ast"
	"github.com/influxdata/kapacitor/tick/stateful"
)

type StreamNode struct {
	node
	s *pipeline.StreamNode
}

// Create a new  StreamNode which copies all data to children
func newStreamNode(et *ExecutingTask, n *pipeline.StreamNode, l *log.Logger) (*StreamNode, error) {
	sn := &StreamNode{
		node: node{Node: n, et: et, logger: l},
		s:    n,
	}
	sn.node.runF = sn.runSourceStream
	return sn, nil
}

func (s *StreamNode) runSourceStream([]byte) error {
	for m, ok := s.ins[0].Emit(); ok; m, ok = s.ins[0].Emit() {
		for _, child := range s.outs {
			err := child.Collect(m)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

type FromNode struct {
	node
	s             *pipeline.FromNode
	expression    stateful.Expression
	scopePool     stateful.ScopePool
	tagNames      []string
	allDimensions bool
	db            string
	rp            string
	name          string
}

// Create a new  FromNode which filters data from a source.
func newFromNode(et *ExecutingTask, n *pipeline.FromNode, l *log.Logger) (*FromNode, error) {
	sn := &FromNode{
		node: node{Node: n, et: et, logger: l},
		s:    n,
		db:   n.Database,
		rp:   n.RetentionPolicy,
		name: n.Measurement,
	}
	sn.node.runF = sn.runStream
	sn.allDimensions, sn.tagNames = determineTagNames(n.Dimensions, nil)

	if n.Lambda != nil {
		expr, err := stateful.NewExpression(n.Lambda.Expression)
		if err != nil {
			return nil, fmt.Errorf("Failed to compile from expression: %v", err)
		}

		sn.expression = expr
		sn.scopePool = stateful.NewScopePool(ast.FindReferenceVariables(n.Lambda.Expression))
	}

	return sn, nil
}

func (s *FromNode) runStream([]byte) error {
	consumer := edge.NewConsumerWithReceiver(
		s.ins[0],
		edge.NewReceiverFromForwardReceiverWithStats(
			s.outs,
			s,
		),
	)
	return consumer.Consume()
}
func (s *FromNode) BeginBatch(edge.BeginBatchMessage) (edge.Message, error) {
	return nil, errors.New("from does not support batch data")
}
func (s *FromNode) BatchPoint(edge.BatchPointMessage) (edge.Message, error) {
	return nil, errors.New("from does not support batch data")
}
func (s *FromNode) EndBatch(edge.EndBatchMessage) (edge.Message, error) {
	return nil, errors.New("from does not support batch data")
}

func (s *FromNode) Point(p edge.PointMessage) (edge.Message, error) {
	if s.matches(p) {
		p = p.ShallowCopy()
		if s.s.Truncate != 0 {
			p.SetTime(p.Time().Truncate(s.s.Truncate))
		}
		if s.s.Round != 0 {
			p.SetTime(p.Time().Round(s.s.Round))
		}
		p.SetDimensions(models.Dimensions{
			ByName:   s.s.GroupByMeasurementFlag,
			TagNames: computeTagNames(p.Tags(), s.allDimensions, s.tagNames, nil),
		})
		return p, nil
	}
	return nil, nil
}

func (s *FromNode) Barrier(b edge.BarrierMessage) (edge.Message, error) {
	return b, nil
}

func (s *FromNode) matches(p edge.PointMessage) bool {
	if s.db != "" && p.Database() != s.db {
		return false
	}
	if s.rp != "" && p.RetentionPolicy() != s.rp {
		return false
	}
	if s.name != "" && p.Name() != s.name {
		return false
	}
	if s.expression != nil {
		if pass, err := EvalPredicate(s.expression, s.scopePool, p); err != nil {
			s.incrementErrorCount()
			s.logger.Println("E! error while evaluating WHERE expression:", err)
			return false
		} else {
			return pass
		}
	}
	return true
}

func setGroupOnPoint(p models.Point, allDimensions bool, dimensions models.Dimensions, excluded []string) models.Point {
	if allDimensions {
		dimensions.TagNames = filterExcludedTagNames(models.SortedKeys(p.Tags), excluded)
	}
	p.Group = models.ToGroupID(p.Name, p.Tags, dimensions)
	p.Dimensions = dimensions
	return p
}
