package plan

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

// ExplainLineage renders the validated lineage required to compute target.
// It inspects metadata only and performs no source reads or execution.
func (g *RDDGraph) ExplainLineage(target RDDID) (string, error) {
	if err := g.Validate(target); err != nil {
		return "", err
	}

	reachable := make(map[RDDID]bool)
	g.collectAncestors(target, reachable)
	ids := make([]RDDID, 0, len(reachable))
	for id := range reachable {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	var output strings.Builder
	output.WriteString("== RDD Lineage ==\n")
	for _, id := range ids {
		writeNodeExplanation(&output, g.nodes[id])
	}
	return output.String(), nil
}

func (g *RDDGraph) collectAncestors(id RDDID, reachable map[RDDID]bool) {
	if reachable[id] {
		return
	}
	reachable[id] = true
	for _, dependency := range g.nodes[id].Dependencies {
		g.collectAncestors(dependency.ParentID, reachable)
	}
}

func writeNodeExplanation(output *strings.Builder, node RDDNode) {
	fmt.Fprintf(
		output,
		"RDD %d %s[%d] operator=%s",
		node.ID,
		node.Name,
		node.NumPartitions,
		node.Operator.Kind,
	)
	if node.Operator.SourcePath != "" {
		fmt.Fprintf(output, " source=%s", strconv.Quote(node.Operator.SourcePath))
	}
	if node.Operator.FunctionID != "" {
		fmt.Fprintf(output, " function=%s", strconv.Quote(node.Operator.FunctionID))
	}
	if node.Partitioner != nil {
		fmt.Fprintf(
			output,
			" partitioner=%s[%d]",
			node.Partitioner.Kind,
			node.Partitioner.NumPartitions,
		)
	}
	for _, dependency := range node.Dependencies {
		fmt.Fprintf(
			output,
			" %s <- RDD %d",
			dependency.Kind,
			dependency.ParentID,
		)
		if dependency.Shuffle != nil {
			fmt.Fprintf(
				output,
				" shuffle=%d shuffle_partitioner=%s[%d] aggregator=%s map_side_combine=%t",
				dependency.Shuffle.ShuffleID,
				dependency.Shuffle.Partitioner.Kind,
				dependency.Shuffle.Partitioner.NumPartitions,
				strconv.Quote(dependency.Shuffle.AggregatorID),
				dependency.Shuffle.MapSideCombine,
			)
		}
	}
	output.WriteByte('\n')
}
