package main

import (
	"github.com/gmbyapa/kafka-connector/connector"
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
	config := new(connector.Config)
	config.Configs = make(map[string]interface{})
	config.Configs[`firestore.credentials.file.path`] = `/home/noel/Dev/go_projects/src/github.com/noelyahan/kafka-connect/kafka-connect-firestore/test-budget-4f14aad07b9b.json`
	config.Configs[`firestore.project.id`] = `test-budget-5529f`
	config.Configs[`firestore.collection`] = `firesink`
	config.Configs[`topics`] = `userTopic,blah-t`
	config.Configs[`firestore.collection.userTopic`] = `students`
	sink.Configure(config)
	sink.Init()
	recs := make([]connector.Recode, 0)
	recs = append(recs, rec{`userTopic`, ``, `{"first":"Noel","last":"Yahan","born":1815}`})
	recs = append(recs, rec{`userTopic`, ``, `{"first":"Noel","last":"Yahan","born":1815}`})
	recs = append(recs, rec{`userTopic`, ``, `{"first":"Noel","last":"Yahan","born":1815}`})

	for i := 0; i < 1000; i++ {
		r := rec{`userTopic`, ``, `{"first":"Noel","last":"Yahan","born":1815}`}
		recs = append(recs, r)
	}
	sink.Process(recs)
}
