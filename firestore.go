package main

import (
	"cloud.google.com/go/firestore"
	"context"
	"encoding/json"
	"fmt"
	"github.com/gmbyapa/kafka-connector/connector"
	"github.com/pickme-go/log"
	"google.golang.org/api/option"
	"strings"
	"sync"
)

const topics  = `topics`
const credentialsFilePath = `firestore.credentials.file.path`
const credentialsFileJson = `firestore.credentials.file.json`
const projectId = `firestore.project.id`
const collection = `firestore.collection`
const collections = `firestore.collections`

var Connector connector.Connector = new(FireConnector)

var Task connector.SinkTaskBuilder = new(storeBuilder)

type FireConnector struct {}
func (f *FireConnector) Init(configs *connector.Config) error { return nil }
func (f *FireConnector) Pause() error { return nil }
func (f *FireConnector) Name() string {
	return `firestore-connector`
}
func (f *FireConnector) Resume() error { return nil }
func (f *FireConnector) Type() connector.ConnectType {
	return connector.ConnetTypeSink
}
func (f *FireConnector) Start() error { return nil }
func (f *FireConnector) Stop() error { return nil }

type storeBuilder struct {
	config  sync.Map
	client  *firestore.Client
}

var fireStoreLogPrefix = `FireStore Sink`

func (f *storeBuilder) get(key string) interface{} {
	res, _ := f.config.Load(key)
	return res
}

func (f *storeBuilder) set(key string, value interface{}) {
	f.config.Store(key, value)
}

func (f *storeBuilder) Configure(config *connector.Config) {
	var conf interface{}
	conf = config.Configs[topics]
	if conf == nil {
		msg := fmt.Sprintf(`%v conifig canot be empty`, topics)
		log.Error(log.WithPrefix(fireStoreLogPrefix, msg))
		panic(msg)
	}
	f.set(topics, conf)

	conf = config.Configs[credentialsFilePath]
	if conf == nil {
		msg := fmt.Sprintf(`%v conifig canot be empty`, credentialsFilePath)
		log.Error(log.WithPrefix(fireStoreLogPrefix, msg))
		panic(msg)
	}
	f.set(credentialsFilePath, conf)

	conf = config.Configs[credentialsFileJson]
	if conf == nil && config.Configs[credentialsFilePath] == nil {
		msg := fmt.Sprintf(`%v conifig canot be empty`, credentialsFileJson)
		log.Error(log.WithPrefix(fireStoreLogPrefix, msg))
		panic(msg)
	}
	f.set(credentialsFileJson, conf)

	conf = config.Configs[projectId]
	if conf == nil {
		msg := fmt.Sprintf(`%v conifig canot be empty`, projectId)
		log.Error(log.WithPrefix(fireStoreLogPrefix, msg))
		panic(msg)
	}
	f.set(projectId, conf)

	if config.Configs[collection] != nil || config.Configs[collections] != nil {
		msg := fmt.Sprintf(`%v conifig canot be empty`, collection)
		log.Error(log.WithPrefix(fireStoreLogPrefix, msg))
		panic(msg)
	}
	f.set(collection, config.Configs[collection])

	// topics and collection mapping - optional
	f.set(collections, config.Configs[collections])
	topics := strings.Split(config.Configs[topics].(string), ",")

	for _, t := range topics {
		key := fmt.Sprintf(`%v.%v`, collection, t)
		col := config.Configs[key]
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
	}else if credFileJSON != nil {
		opt = option.WithCredentialsJSON([]byte(credFileJSON.(string)))
	}
	projectId := f.get(projectId).(string)
	client, err := firestore.NewClient(ctx, projectId, opt)
	if err != nil {
		log.Error(log.WithPrefix(fireStoreLogPrefix, fmt.Sprintf("canot connect to firestore, error on creating a client: %v", err)))
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
		log.Error(log.WithPrefix(fireStoreLogPrefix, fmt.Sprintf("error on closinf the client: %v", err)))
		return err
	}
	return nil
}

func (f *storeBuilder) OnRebalanced() connector.ReBalanceHandler { return nil }

func (f *storeBuilder) Process(records []connector.Recode) error {
	ctx := context.Background()
	for _, rec := range records {
		// single collection multiple topic mapping
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
			return err
		}
		_, _, err = f.client.Collection(collection.(string)).Add(ctx, mapCol)
		if err != nil {
			return err
		}
	}

	return nil
}

func (f *storeBuilder) Build() (connector.SinkTask, error)  {
	return new(storeBuilder), nil
}