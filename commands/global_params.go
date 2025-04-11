package commands

import "github.com/xitonix/trubka/internal"

// GlobalParameters holds the app's global parameters available to all the sub-commands.
type GlobalParameters struct {
	// Verbosity logging verbosity level.
	Verbosity internal.VerbosityLevel
	// EnableColor enables colours across all the sub-commands.
	EnableColor bool
	// Prefer compact output
	Compact bool
}
