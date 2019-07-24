package main

import (
	"github.com/gmbyapa/kafka-connector/connector"
	"github.com/pickme-go/log"
	"github.com/pickme-go/metrics"
	"testing"
	"time"
)
type rec struct {
	topic string
	key string
	value string
}

func (r rec) Topic() string {return r.topic}
func (r rec) Partition() int32 {return 0}
func (r rec) Offset() int64 { return 0}
func (r rec) Key() interface{} {return r.key}
func (r rec) Value() interface{} {return r.value}
func (r rec) Timestamp() time.Time {return time.Now()}
/*
const topics  = `topics`
const credentialsFilePath = `firestore.credentials.file.path`
const credentialsFileJson = `firestore.credentials.file.json`
const projectId = `firestore.project.id`
const collection = `firestore.collection`
const collections = `firestore.collections`
*/
func TestFireStore_Sink(t *testing.T) {
	sink, _ := Task.Build()
	config := new(connector.TaskConfig)
	config.Logger = log.NewPrefixedNoopLogger()
	config.Connector =  new(connector.Config)
	config.Connector.Metrics =  metrics.NoopReporter()
	config.Connector.Logger =  log.NewPrefixedNoopLogger()
	config.Connector.Configs = make(map[string]interface{})
	config.Connector.Configs[`firestore.credentials.file.path`] = `/home/noel/Dev/go_projects/src/github.com/noelyahan/kafka-connect/kafka-connect-firestore/test-budget-4f14aad07b9b.json`
	config.Connector.Configs[`firestore.project.id`] = `test-budget-5529f`
	config.Connector.Configs[`firestore.collection`] = `firesink`
	config.Connector.Configs[`topics`] = `userTopic,blah-t`
	config.Connector.Configs[`firestore.collection.userTopic`] = `students`
	config.Connector.Configs[`firestore.topic.pk.collections`] = `students`
	sink.Configure(config)
	sink.Init()
	recs := make([]connector.Recode, 0)
	recs = append(recs, rec{`userTopic`, `123`, `{"first":"Noel","last":"Yahan","born":1815}`})
	recs = append(recs, rec{`userTopic`, `111`, `{"first":"Noel","last":"Yahan","born":1815}`})
	recs = append(recs, rec{`userTopic`, `222`, `{"first":"Noel","last":"Yahan","born":1815}`})

	//for i := 0; i < 1000; i++ {
	//	r := rec{`userTopic`, ``, `{"first":"Noel","last":"Yahan","born":1815}`}
	//	recs = append(recs, r)
	//}
	sink.(connector.SinkTask).Process(recs)
}
