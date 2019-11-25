package internal

import "fmt"

type Counter struct {
	success uint64
	failure uint64
}

func (c *Counter) Print(highlight bool) {
	failed := RedIfTrue(c.failure, func() bool {
		return c.failure > 0
	}, highlight)

	succeeded := GreenIfTrue(c.success, func() bool {
		return c.success > 0
	}, highlight)
	fmt.Printf("\nSummary:  \n  Succeeded: %s\n     Failed: %s", succeeded, failed)
}

func (c *Counter) IncrSuccess() {
	c.success++
}

func (c *Counter) IncrFailure() {
	c.failure++
}
