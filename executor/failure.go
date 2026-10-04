package executor

import (
	"fmt"

	"github.com/Wendyddw/sparkcore-go/scheduler"
)

func permanentErrorf(format string, args ...any) error {
	return scheduler.PermanentFailure(fmt.Errorf(format, args...))
}
