package main

import (
	"bitbucket.org/mybudget-dev/stream-connect-worker/connector"
	"github.com/pickme-go/log"
	"github.com/pickme-go/metrics"
	"strings"
	"testing"
	"time"
)

type rec struct {
	topic string
	key   interface{}
	value interface{}
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
	config.Connector.Configs[`topics`] = `account,blah-t`
	config.Connector.Configs[`firestore.collection.account`] = `accounts/${account_id}/goals/${goal_id}`
	config.Connector.Configs[`firestore.topic.pk.collections`] = `accounts/${account_id}/goals/${goal_id}`
	err := sink.Init(config)
	if err != nil {
		t.Fatal(err)
	}
	recs := make([]connector.Recode, 0)
	recs = append(recs, rec{`account`, `111`, nil})
	recs = append(recs, rec{`account`, `111`, `{"first":"Test 11","last":"Test","born":1111,"account_id":"a111","goal_id":"g111"}`})
	//recs = append(recs, rec{`account`, `222`, `{"first":"Test 2","last":"Test","born":1815,"account_id":"a111","goal_id":"g222"}`})
	//recs = append(recs, rec{`account`, `333`, `{"first":"Test 3","last":"Test","born":1815,"account_id":"a111","goal_id":"g333"}`})

	//for i := 0; i < 3; i++ {
	//	r := rec{`userTopic`, ``, `{"first":"Noel","last":"Yahan","born":1815}`}
	//	recs = append(recs, r)
	//}firestore: nil DocumentRef
	err = sink.(connector.SinkTask).Process(recs)
	if err != nil {
		t.Fatal(err)
	}
}

func TestFireConnector_GetCollectionPath(t *testing.T) {
	col := `accounts/${id}/goals`
	val := `{"first":"Test 11","last":"Test","born":1111,"id":"a111"}`
	arr := strings.Split(col, "/")
	t.Log(arr)
	res := new(task).getCollPath(arr, val)
	t.Log(res)
	t.Log(arr)
}
