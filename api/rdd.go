package api

import "github.com/Wendyddw/sparkcore-go/plan"

// RDD is a lightweight handle to an immutable lineage node.
// Data remains unevaluated until an action runs through the owning context.
type RDD struct {
	id  plan.RDDID
	ctx *Context
}
