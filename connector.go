package main

import (
	"bitbucket.org/mybudget-dev/stream-connect-worker/connector"
)

var Connector connector.Connector = new(fireConnector)

type fireConnector struct{}

func (f *fireConnector) Init(configs *connector.Config) error {
	return nil
}

func (f *fireConnector) Name() string {
	return `firestore-connector`
}

func (f *fireConnector) Type() connector.ConnectType {
	return connector.ConnectTypeSink
}

func (f *fireConnector) Builder() connector.TaskBuilder {
	return new(taskBuilder)
}
