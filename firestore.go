package main

import (
	"cloud.google.com/go/firestore"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gmbyapa/kafka-connector/connector"
	"github.com/pickme-go/metrics"
	"google.golang.org/api/option"
	"strings"
	"sync"
	"time"
)

const topics = `topics`
const credentialsFilePath = `firestore.credentials.file.path`
const credentialsFileJson = `firestore.credentials.file.json`
const projectId = `firestore.project.id`
const collection = `firestore.collection`

var Connector connector.Connector = new(FireConnector)

var Task connector.TaskBuilder = new(storeBuilder)

type FireConnector struct {
}

func (f *FireConnector) Init(configs *connector.Config) error {

	return nil
}
func (f *FireConnector) Pause() error { return nil }
func (f *FireConnector) Name() string {
	return `firestore-connector`
}
func (f *FireConnector) Resume() error { return nil }
func (f *FireConnector) Type() connector.ConnectType {
	return connector.ConnectTypeSink
}
func (f *FireConnector) Start() error { return nil }
func (f *FireConnector) Stop() error  { return nil }

type storeBuilder struct {
	log     connector.Logger
	config  sync.Map
	client  *firestore.Client
	latency metrics.Observer
}

var fireStoreLogPrefix = `FireStore Sink`

func (f *storeBuilder) get(key string) interface{} {
	res, _ := f.config.Load(key)
	return res
}

func (f *storeBuilder) set(key string, value interface{}) {
	f.config.Store(key, value)
}

func (f *storeBuilder) Configure(config *connector.TaskConfig) {
	f.log = config.Logger
	f.latency = config.Connector.Metrics.Observer(metrics.MetricConf{
		Path:        `firestore_sink_connector`,
		Labels:      []string{`collection`},
		ConstLabels: map[string]string{`task`: config.TaskId},
	})
	var conf interface{}
	conf = config.Connector.Configs[topics]
	if conf == nil {
		msg := fmt.Sprintf(`%v conifig canot be empty`, topics)
		f.log.Error(fireStoreLogPrefix, msg)
	}
	f.set(topics, conf)

	conf = config.Connector.Configs[credentialsFilePath]
	f.set(credentialsFilePath, conf)

	conf = config.Connector.Configs[credentialsFileJson]
	if conf == nil && config.Connector.Configs[credentialsFilePath] == nil {
		msg := fmt.Sprintf(`%v conifig canot be empty`, credentialsFileJson)
		f.log.Error(fireStoreLogPrefix, msg)
	}
	f.set(credentialsFileJson, conf)

	conf = config.Connector.Configs[projectId]
	if conf == nil {
		msg := fmt.Sprintf(`%v conifig canot be empty`, projectId)
		f.log.Error(fireStoreLogPrefix, msg)
	}
	f.set(projectId, conf)

	f.set(collection, config.Connector.Configs[collection])

	// topics and collection mapping - optional
	topics := strings.Split(config.Connector.Configs[topics].(string), ",")

	for _, t := range topics {
		key := fmt.Sprintf(`%v.%v`, collection, t)
		col := config.Connector.Configs[key]
		if col != nil {
			f.set(t, col.(string))
		}
	}
}
func (f *storeBuilder) Init() error {
	ctx := context.Background()
	var opt option.ClientOption
	credFilePath := f.get(credentialsFilePath)
	credFileJSON := f.get(credentialsFileJson)
	if credFilePath != nil {
		opt = option.WithCredentialsFile(credFilePath.(string))
	} else if credFileJSON != nil {
		b, err := json.Marshal(credFileJSON)
		if err != nil {
			f.log.Error(fireStoreLogPrefix, fmt.Sprintf("canot convert json firestore credentials"))
			return err
		}
		opt = option.WithCredentialsJSON(b)
	}
	projectId := f.get(projectId).(string)
	client, err := firestore.NewClient(ctx, projectId, opt)
	if err != nil {
		f.log.Error(fireStoreLogPrefix, fmt.Sprintf("canot connect to firestore, error on creating a client: %v", err))
		return err
	}
	f.client = client
	return nil
}
func (f *storeBuilder) Start() error {
	return nil
}
func (f *storeBuilder) Stop() error {
	err := f.client.Close()
	if err != nil {
		f.log.Error(fireStoreLogPrefix, fmt.Sprintf("error on closinf the client: %v", err))
		return err
	}
	return nil
}

func (f *storeBuilder) OnRebalanced() connector.ReBalanceHandler { return nil }

func (f *storeBuilder) Process(records []connector.Recode) error {
	ctx := context.Background()
	for _, rec := range records {
		// single collection multiple topic mapping
		begin := time.Now()
		store(ctx, rec, f)
		f.latency.Observe(float64(begin.UnixNano()), nil)
	}

	return nil
}

func store(ctx context.Context, rec connector.Recode, f *storeBuilder)  {
	collection := f.get(collection)
	if collection == nil {
		collection = rec.Topic()
	}
	// topic collection mapping info
	col := f.get(rec.Topic())
	if col != nil {
		collection = col
	}
	mapCol := make(map[string]interface{})
	err := json.Unmarshal([]byte(rec.Value().(string)), &mapCol)
	if err != nil {
		f.log.Error(fireStoreLogPrefix, fmt.Sprintf("could not create the payload: %v", err))
		return
	}
	_, _, err = f.client.Collection(collection.(string)).Add(ctx, mapCol)
	if err != nil {
		f.log.Error(fireStoreLogPrefix, fmt.Sprintf("could not store to firestore: %v", err))
		return
	}
	f.log.Debug(fireStoreLogPrefix, fmt.Sprintf("firestore message insert done: %v", rec.Value().(string)))
}

func (f *storeBuilder) Build() (connector.Task, error) {
	return new(storeBuilder), nil
}
