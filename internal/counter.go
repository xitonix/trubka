package internal

import "fmt"

type Counter struct {
	success uint64
	failure uint64
}

func (c *Counter) Print() {
	failed := RedIfTrue(c.failure, func() bool {
		return c.failure > 0
	})

	succeeded := GreenIfTrue(c.success, func() bool {
		return c.success > 0
	})
	fmt.Printf("\nSummary:  \n  Succeeded: %s\n     Failed: %s", succeeded, failed)
}

func (c *Counter) IncrSuccess() {
	c.success++
}

func (c *Counter) IncrFailure() {
	c.failure++
}
