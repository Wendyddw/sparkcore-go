package plan

import "fmt"

// RDDGraph owns the immutable RDD lineage for one driver process.
// It is not safe for concurrent use; the driver serializes mutations.
type RDDGraph struct {
	nextRDDID RDDID
	nodes     map[RDDID]RDDNode
}

// NewRDDGraph creates an empty lineage graph.
func NewRDDGraph() *RDDGraph {
	return &RDDGraph{
		nodes: make(map[RDDID]RDDNode),
	}
}

// AddNode validates, copies, and stores a node under a new ID.
func (g *RDDGraph) AddNode(node RDDNode) (RDDID, error) {
	if err := validateNodeShape(node); err != nil {
		return 0, err
	}
	if g.nodes == nil {
		g.nodes = make(map[RDDID]RDDNode)
	}

	id := g.nextRDDID
	g.nextRDDID++

	node.ID = id
	g.nodes[id] = cloneRDDNode(node)
	return id, nil
}

// Node returns a copy of the requested node.
func (g *RDDGraph) Node(id RDDID) (RDDNode, bool) {
	node, ok := g.nodes[id]
	if !ok {
		return RDDNode{}, false
	}
	return cloneRDDNode(node), true
}

// Nodes returns copies of all nodes in ID order.
func (g *RDDGraph) Nodes() []RDDNode {
	nodes := make([]RDDNode, 0, len(g.nodes))
	for id := RDDID(0); id < g.nextRDDID; id++ {
		if node, ok := g.nodes[id]; ok {
			nodes = append(nodes, cloneRDDNode(node))
		}
	}
	return nodes
}

// Len returns the number of nodes in the graph.
func (g *RDDGraph) Len() int {
	return len(g.nodes)
}

func validateNodeShape(node RDDNode) error {
	if node.NumPartitions <= 0 {
		return fmt.Errorf("RDD %q has invalid partition count %d", node.Name, node.NumPartitions)
	}
	if node.Partitioner != nil {
		if err := validatePartitioner(*node.Partitioner); err != nil {
			return fmt.Errorf("RDD %q partitioner: %w", node.Name, err)
		}
		if node.Partitioner.NumPartitions != node.NumPartitions {
			return fmt.Errorf(
				"RDD %q has %d partitions but its partitioner has %d",
				node.Name,
				node.NumPartitions,
				node.Partitioner.NumPartitions,
			)
		}
	}

	for i, dependency := range node.Dependencies {
		if err := validateDependencyShape(dependency); err != nil {
			return fmt.Errorf("RDD %q dependency %d: %w", node.Name, i, err)
		}
	}
	return nil
}

func validateDependencyShape(dependency Dependency) error {
	switch dependency.Kind {
	case DependencyNarrow:
		if dependency.Narrow == nil || dependency.Shuffle != nil {
			return fmt.Errorf("narrow dependency must contain only a narrow specification")
		}
		if dependency.Narrow.Mapping != NarrowOneToOne {
			return fmt.Errorf("unsupported narrow mapping %q", dependency.Narrow.Mapping)
		}
	case DependencyShuffle:
		if dependency.Shuffle == nil || dependency.Narrow != nil {
			return fmt.Errorf("shuffle dependency must contain only a shuffle specification")
		}
		if err := validatePartitioner(dependency.Shuffle.Partitioner); err != nil {
			return fmt.Errorf("shuffle partitioner: %w", err)
		}
	default:
		return fmt.Errorf("unsupported dependency kind %q", dependency.Kind)
	}
	return nil
}

func validatePartitioner(partitioner PartitionerSpec) error {
	if partitioner.Kind != PartitionerHash {
		return fmt.Errorf("unsupported partitioner kind %q", partitioner.Kind)
	}
	if partitioner.NumPartitions <= 0 {
		return fmt.Errorf("invalid partition count %d", partitioner.NumPartitions)
	}
	return nil
}

// Deep copies preserve immutable lineage by preventing callers from retaining
// references to graph-owned slices or pointed-to metadata.
func cloneRDDNode(node RDDNode) RDDNode {
	clone := node
	if node.Partitioner != nil {
		partitioner := *node.Partitioner
		clone.Partitioner = &partitioner
	}
	if node.Dependencies != nil {
		clone.Dependencies = make([]Dependency, len(node.Dependencies))
		for i, dependency := range node.Dependencies {
			clone.Dependencies[i] = cloneDependency(dependency)
		}
	}
	return clone
}

func cloneDependency(dependency Dependency) Dependency {
	clone := dependency
	if dependency.Narrow != nil {
		narrow := *dependency.Narrow
		clone.Narrow = &narrow
	}
	if dependency.Shuffle != nil {
		shuffle := *dependency.Shuffle
		clone.Shuffle = &shuffle
	}
	return clone
}
