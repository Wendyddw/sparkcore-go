package scheduler

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/Wendyddw/sparkcore-go/plan"
)

// ExplainStagePlan renders a deterministic, human-readable stage DAG.
func ExplainStagePlan(plan StagePlan) string {
	var output strings.Builder
	output.WriteString("== Stage DAG ==\n")
	fmt.Fprintf(&output, "Target RDD %d\n", plan.TargetRDD)

	for _, stage := range plan.Stages {
		fmt.Fprintf(
			&output,
			"Stage %d kind=%s partitions=%d parents=%s tasks=%d\n",
			stage.ID,
			stage.Kind,
			stage.NumPartitions,
			formatStageIDs(stage.ParentIDs),
			stage.NumPartitions,
		)
		fmt.Fprintf(&output, "  Pipeline: %s\n", formatStageOperations(stage.Operations))
		if stage.ShuffleWrite != nil {
			fmt.Fprintf(
				&output,
				"  ShuffleWrite: shuffle=%d partitioner=%s[%d] aggregator=%s map_side_combine=%t\n",
				stage.ShuffleWrite.ShuffleID,
				stage.ShuffleWrite.Partitioner.Kind,
				stage.ShuffleWrite.Partitioner.NumPartitions,
				strconv.Quote(stage.ShuffleWrite.AggregatorID),
				stage.ShuffleWrite.MapSideCombine,
			)
		}
		if stage.FinalAction != nil {
			fmt.Fprintf(
				&output,
				"  Action: %s target=RDD %d\n",
				stage.FinalAction.Kind,
				stage.FinalAction.TargetRDD,
			)
		}
	}

	return output.String()
}

func formatStageIDs(ids []plan.StageID) string {
	if len(ids) == 0 {
		return "[]"
	}

	values := make([]string, len(ids))
	for i, id := range ids {
		values[i] = strconv.FormatInt(int64(id), 10)
	}
	return "[" + strings.Join(values, ",") + "]"
}

func formatStageOperations(operations []StageOperation) string {
	values := make([]string, 0, len(operations))
	for _, operation := range operations {
		switch operation.Kind {
		case StageOperationRDD:
			if operation.RDD != nil {
				values = append(values, fmt.Sprintf("RDD %d %s", operation.RDD.RDDID, operation.RDD.Operator.Kind))
			}
		case StageOperationShuffleRead:
			if operation.ShuffleRead != nil {
				values = append(values, fmt.Sprintf(
					"ShuffleRead %d %s[%d]",
					operation.ShuffleRead.ShuffleID,
					operation.ShuffleRead.Partitioner.Kind,
					operation.ShuffleRead.Partitioner.NumPartitions,
				))
			}
		}
	}
	return strings.Join(values, " -> ")
}
