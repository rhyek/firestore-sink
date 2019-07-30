package main

import "bitbucket.org/mybudget-dev/stream-connect-worker/connector"

type taskBuilder struct{}

func (t *taskBuilder) Build() (connector.Task, error) {
	return new(task), nil
}
