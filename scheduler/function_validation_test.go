package scheduler

import (
	"strings"
	"testing"

	"github.com/Wendyddw/sparkcore-go/plan"
)

func TestValidateFunctionsResolvesEachOperatorKind(t *testing.T) {
	tests := []struct {
		name       string
		nodeName   string
		kind       plan.OperatorKind
		dependency plan.DependencyKind
		wantLookup string
	}{
		{name: "map", nodeName: "Map", kind: plan.OpMap, dependency: plan.DependencyNarrow, wantLookup: "map:function"},
		{name: "filter", nodeName: "Filter", kind: plan.OpFilter, dependency: plan.DependencyNarrow, wantLookup: "filter:function"},
		{name: "pair map", nodeName: "MapToPair", kind: plan.OpMapToPair, dependency: plan.DependencyNarrow, wantLookup: "pair-map:function"},
		{name: "value map", nodeName: "MapValues", kind: plan.OpMapValues, dependency: plan.DependencyNarrow, wantLookup: "value-map:function"},
		{name: "reduce", nodeName: "ReduceByKey", kind: plan.OpReduceByKey, dependency: plan.DependencyShuffle, wantLookup: "reduce:function"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			graph := plan.NewRDDGraph()
			parent := addValidationNode(t, graph, validationSource())
			target := addValidationNode(
				t,
				graph,
				validationFunctionNode(test.nodeName, test.kind, "function", parent, test.dependency),
			)
			lookup := &recordingFunctionLookup{exists: true}

			if err := validateFunctions(graph, target, lookup); err != nil {
				t.Fatalf("validateFunctions() error = %v", err)
			}
			if len(lookup.calls) != 1 || lookup.calls[0] != test.wantLookup {
				t.Fatalf("lookup calls = %#v, want [%q]", lookup.calls, test.wantLookup)
			}
		})
	}
}

func TestValidateFunctionsReturnsDescriptiveLookupError(t *testing.T) {
	graph := plan.NewRDDGraph()
	parent := addValidationNode(t, graph, validationSource())
	target := addValidationNode(
		t,
		graph,
		validationFunctionNode("Map", plan.OpMap, "missing-map", parent, plan.DependencyNarrow),
	)
	lookup := &recordingFunctionLookup{}

	err := validateFunctions(graph, target, lookup)
	for _, part := range []string{"RDD 1", "Map", "missing-map", "not registered"} {
		if !strings.Contains(err.Error(), part) {
			t.Errorf("validateFunctions() error = %q, want it to contain %q", err, part)
		}
	}
}

func TestValidateFunctionsIgnoresUnreachableOperators(t *testing.T) {
	graph := plan.NewRDDGraph()
	target := addValidationNode(t, graph, validationSource())
	addValidationNode(
		t,
		graph,
		validationFunctionNode("Map", plan.OpMap, "unrelated-map", target, plan.DependencyNarrow),
	)
	lookup := &recordingFunctionLookup{}

	if err := validateFunctions(graph, target, lookup); err != nil {
		t.Fatalf("validateFunctions() error = %v", err)
	}
	if len(lookup.calls) != 0 {
		t.Fatalf("lookup calls = %#v, want none", lookup.calls)
	}
}

func TestValidateFunctionsRejectsNilLookup(t *testing.T) {
	graph := plan.NewRDDGraph()
	target := addValidationNode(t, graph, validationSource())

	err := validateFunctions(graph, target, nil)
	if err == nil || !strings.Contains(err.Error(), "function lookup is nil") {
		t.Fatalf("validateFunctions() error = %v, want nil lookup error", err)
	}
}

type recordingFunctionLookup struct {
	calls  []string
	exists bool
}

func (l *recordingFunctionLookup) HasMap(id string) bool {
	l.calls = append(l.calls, "map:"+id)
	return l.exists
}

func (l *recordingFunctionLookup) HasFilter(id string) bool {
	l.calls = append(l.calls, "filter:"+id)
	return l.exists
}

func (l *recordingFunctionLookup) HasPairMap(id string) bool {
	l.calls = append(l.calls, "pair-map:"+id)
	return l.exists
}

func (l *recordingFunctionLookup) HasValueMap(id string) bool {
	l.calls = append(l.calls, "value-map:"+id)
	return l.exists
}

func (l *recordingFunctionLookup) HasReduce(id string) bool {
	l.calls = append(l.calls, "reduce:"+id)
	return l.exists
}

func validationSource() plan.RDDNode {
	return plan.RDDNode{
		Name:          "TextFile",
		Operator:      plan.OperatorSpec{Kind: plan.OpSource, SourcePath: "events.txt"},
		NumPartitions: 2,
	}
}

func validationFunctionNode(
	name string,
	kind plan.OperatorKind,
	functionID string,
	parent plan.RDDID,
	dependencyKind plan.DependencyKind,
) plan.RDDNode {
	dependency := plan.Dependency{Kind: dependencyKind, ParentID: parent}
	var partitioner *plan.PartitionerSpec
	if dependencyKind == plan.DependencyNarrow {
		dependency.Narrow = &plan.NarrowDependencySpec{Mapping: plan.NarrowOneToOne}
	} else {
		requested := plan.HashPartitioner(2)
		partitioner = &requested
		dependency.Shuffle = &plan.ShuffleDependencySpec{
			Partitioner:  requested,
			AggregatorID: functionID,
		}
	}
	return plan.RDDNode{
		Name:          name,
		Operator:      plan.OperatorSpec{Kind: kind, FunctionID: functionID},
		Dependencies:  []plan.Dependency{dependency},
		NumPartitions: 2,
		Partitioner:   partitioner,
	}
}

func addValidationNode(t *testing.T, graph *plan.RDDGraph, node plan.RDDNode) plan.RDDID {
	t.Helper()
	id, err := graph.AddNode(node)
	if err != nil {
		t.Fatalf("AddNode(%s) error = %v", node.Name, err)
	}
	return id
}
