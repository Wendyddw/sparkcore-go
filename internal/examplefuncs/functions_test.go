package examplefuncs

import (
	"context"
	"encoding/json"
	"math"
	"testing"

	"github.com/Wendyddw/sparkcore-go/executor"
	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func TestSumIntExactNumbersAndOverflow(t *testing.T) {
	registry := executor.NewFunctionRegistry()
	if err := Register(registry); err != nil {
		t.Fatal(err)
	}
	sum, err := registry.Reduce("sum_int")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		a, b executor.Record
		want int64
	}{
		{1, int64(2), 3},
		{json.Number("9007199254740993"), 2, 9007199254740995},
		{json.Number("-9223372036854775808"), json.Number("9223372036854775807"), -1},
		{int64(math.MaxInt64), 0, math.MaxInt64},
		{int64(math.MinInt64), 0, math.MinInt64},
	} {
		got, err := sum(test.a, test.b)
		if err != nil || got != test.want {
			t.Fatalf("sum(%v,%v) = %v, %v; want %d", test.a, test.b, got, err, test.want)
		}
	}
	for _, invalid := range []executor.Record{nil, true, "1", float64(1), json.Number("1.0"), json.Number("1e2"), json.Number("9223372036854775808"), json.Number("+1"), json.Number("01")} {
		if _, err := sum(invalid, 0); err == nil {
			t.Fatalf("accepted %T(%v)", invalid, invalid)
		}
		if _, err := sum(0, invalid); err == nil {
			t.Fatalf("accepted right operand %T(%v)", invalid, invalid)
		}
	}
	for _, operands := range [][2]int64{{math.MaxInt64, 1}, {math.MinInt64, -1}} {
		if _, err := sum(operands[0], operands[1]); err == nil {
			t.Fatalf("accepted overflow: %v", operands)
		} else if kind, _ := scheduler.ClassifyFailure(context.Background(), err); kind != scheduler.FailurePermanent {
			t.Fatalf("overflow classified %s", kind)
		}
	}
}
