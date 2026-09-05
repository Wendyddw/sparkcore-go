package jobspec

import (
	"fmt"

	"github.com/Wendyddw/sparkcore-go/api"
	"github.com/Wendyddw/sparkcore-go/plan"
)

// Build records the specification as lazy RDD lineage and returns its target.
func Build(ctx *api.Context, spec Spec) (api.RDD, error) {
	if ctx == nil {
		return api.RDD{}, fmt.Errorf("build job: API context is nil")
	}
	if err := spec.Validate(); err != nil {
		return api.RDD{}, err
	}

	rdd, err := ctx.TextFile(spec.Source.Path, spec.Source.NumPartitions)
	if err != nil {
		return api.RDD{}, fmt.Errorf("build job source: %w", err)
	}
	for index, transformation := range spec.Transformations {
		rdd, err = applyTransformation(rdd, transformation)
		if err != nil {
			return api.RDD{}, fmt.Errorf("build job transformation %d: %w", index, err)
		}
	}
	return rdd, nil
}

func applyTransformation(rdd api.RDD, transformation TransformationSpec) (api.RDD, error) {
	switch transformation.Kind {
	case plan.OpMap:
		return rdd.Map(transformation.FunctionID)
	case plan.OpFilter:
		return rdd.Filter(transformation.FunctionID)
	case plan.OpMapToPair:
		return rdd.MapToPair(transformation.FunctionID)
	case plan.OpMapValues:
		return rdd.MapValues(transformation.FunctionID)
	case plan.OpReduceByKey:
		return rdd.ReduceByKey(
			transformation.FunctionID,
			plan.HashPartitioner(transformation.NumPartitions),
		)
	default:
		return api.RDD{}, fmt.Errorf("unsupported operator %q", transformation.Kind)
	}
}
