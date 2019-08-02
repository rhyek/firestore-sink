package main

import (
	"bitbucket.org/mybudget-dev/stream-connect-worker/connector"
	"github.com/pickme-go/log"
	"github.com/pickme-go/metrics"
	"testing"
	"time"
)

type rec struct {
	topic string
	key   string
	value string
}

func (r rec) Topic() string        { return r.topic }
func (r rec) Partition() int32     { return 0 }
func (r rec) Offset() int64        { return 0 }
func (r rec) Key() interface{}     { return r.key }
func (r rec) Value() interface{}   { return r.value }
func (r rec) Timestamp() time.Time { return time.Now() }


func TestFireStore_Sink(t *testing.T) {
	sink, _ := new(taskBuilder).Build()
	config := new(connector.TaskConfig)
	config.Logger = log.NewNoopLogger()
	config.Connector = new(connector.Config)
	config.Connector.Metrics = metrics.NoopReporter()
	config.Connector.Configs = make(map[string]interface{})
	config.Connector.Configs[`firestore.credentials.file.path`] = `/home/noel/Dev/go_projects/src/github.com/noelyahan/kafka-connect/kafka-connect-firestore/test-budget-4f14aad07b9b.json`
	config.Connector.Configs[`firestore.project.id`] = `test-budget-5529f`
	//config.Connector.Configs[`firestore.collection`] = `firesink`
	config.Connector.Configs[`topics`] = `userTopic,blah-t`
	config.Connector.Configs[`firestore.collection.userTopic`] = `students`
	config.Connector.Configs[`firestore.topic.pk.collections`] = `students`
	config.Connector.Configs[`firestore.students.sub.collection`] = `students2`
	sink.Init(config)
	recs := make([]connector.Recode, 0)
	recs = append(recs, rec{`userTopic`, `1sadasd-dasdasd`, `{"first":"Noel","last":"Yahan","born":1815}`})
	recs = append(recs, rec{`userTopic`, `1232313-123123`, `{"first":"Noel","last":"Yahan","born":1815}`})
	recs = append(recs, rec{`userTopic`, `3123123-21312`, `{"first":"Noel","last":"Yahan","born":1815}`})

	//for i := 0; i < 3; i++ {
	//	r := rec{`userTopic`, ``, `{"first":"Noel","last":"Yahan","born":1815}`}
	//	recs = append(recs, r)
	//}
	sink.(connector.SinkTask).Process(recs)
}