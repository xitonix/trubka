package kafka

import "errors"

var (
	// ErrEmptyEnvironment occurs when the provided environment is empty.
	ErrEmptyEnvironment = errors.New("The environment cannot be empty")

	// ErrEmptyTopic occurs when the provided topic is empty.
	ErrEmptyTopic = errors.New("The topic cannot be empty")
)
