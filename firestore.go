package main

import (
	"bitbucket.org/mybudget-dev/stream-connect-worker/connector"
	"cloud.google.com/go/firestore"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pickme-go/metrics"
	"github.com/tidwall/gjson"
	"google.golang.org/api/option"
	"strings"
	"sync"
	"time"
)

const topics = `topics`
const credentialsFilePath = `firestore.credentials.file.path`
const credentialsFileJson = `firestore.credentials.file.json`
const projectId = `firestore.project.id`
const pkMode = `firestore.topic.pk.collections`
const deleteOnNull = `firestore.delete.on.null.values`

var Connector connector.Connector = new(fireConnector)

type fireConnector struct{}

var replacer = strings.NewReplacer("$", "", "{", "", "}", "")

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

type task struct {
	log     connector.Logger
	config  sync.Map
	state   sync.Map
	client  *firestore.Client
	latency metrics.Observer
}

var fireStoreLogPrefix = `FireStore Sink`

func (f *task) get(key string) interface{} {
	res, _ := f.config.Load(key)
	return res
}

func (f *task) set(key string, value interface{}) {
	f.config.Store(key, value)
}

func (f *task) syncState(key, value interface{}) {
	f.state.Store(key, value)
}

func (f *task) getState(key interface{}) interface{} {
	value, _ := f.state.Load(key)
	return value
}

func (f *task) configure(config *connector.TaskConfig) {
	f.log = config.Logger
	f.latency = config.Connector.Metrics.Observer(metrics.MetricConf{
		Path:        `firestore_sink_connector_write_latency_microseconds`,
		Labels:      []string{`collections`},
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
		msg := fmt.Sprintf(`%v conifig could not be empty`, projectId)
		f.log.Error(fireStoreLogPrefix, msg)
	}
	f.set(projectId, conf)

	// topics and collections mapping - optional
	topics := strings.Split(config.Connector.Configs[topics].(string), ",")

	for _, t := range topics {
		t = strings.Replace(t, " ", "", -1)
		key := fmt.Sprintf(`%v.%v`, `firestore.collection`, t)

		col := config.Connector.Configs[key]
		if col != nil {
			f.set(t, col.(string))
		}
	}

	conf = config.Connector.Configs[deleteOnNull]
	if conf == nil {
		f.set(deleteOnNull, false)
	}else {
		f.set(deleteOnNull, conf.(bool))
	}
	conf = config.Connector.Configs[pkMode]
	if conf == nil {
		return
	}
	pkCols := strings.Split(conf.(string), ",")
	for _, c := range pkCols {
		f.set(fmt.Sprintf(`pk/%s`, c), struct{}{})
	}
}
func (f *task) Init(config *connector.TaskConfig) error {
	f.configure(config)
	ctx := context.Background()
	var opt option.ClientOption
	credFilePath := f.get(credentialsFilePath)
	credFileJSON := f.get(credentialsFileJson)
	if credFilePath != nil {
		opt = option.WithCredentialsFile(credFilePath.(string))
	} else if credFileJSON != nil {
		b, err := json.Marshal(credFileJSON)
		if err != nil {
			f.log.Error(fireStoreLogPrefix, fmt.Sprintf("could not convert json firestore credentials"))
			return err
		}
		opt = option.WithCredentialsJSON(b)
	}
	if opt == nil {
		opt = option.WithoutAuthentication()
	}
	projectId := f.get(projectId).(string)
	client, err := firestore.NewClient(ctx, projectId, opt)
	if err != nil {
		f.log.Error(fireStoreLogPrefix, fmt.Sprintf("could not connect to firestore, error on creating a client: %v", err))
		return err
	}
	f.client = client
	return nil
}
func (f *task) Start() error {
	return nil
}
func (f *task) Stop() error {
	err := f.client.Close()
	if err != nil {
		f.log.Error(fireStoreLogPrefix, fmt.Sprintf("error on closinf the client: %v", err))
		return err
	}
	return nil
}

func (f *task) OnRebalanced() connector.ReBalanceHandler { return nil }

func (f *task) Process(records []connector.Recode) error {
	ctx := context.Background()
	for _, rec := range records {
		// single collections multiple topic mapping
		err := f.store(ctx, rec)
		if err != nil {
			f.log.Error(fireStoreLogPrefix, err, rec.Key(), rec.Value())
			continue
		}
		f.log.Trace(`record batch processed`, records)
	}
	return nil
}

func (f *task) store(ctx context.Context, rec connector.Recode) error {
	defer func() {
		// sync state
		f.syncState(rec.Key(), rec)
	}()
	// topic collections mapping info
	var err error
	col := f.get(rec.Topic())
	if col == nil {
		err = fmt.Errorf("firestore col not found \n")
		return err
	}
	defer func(begin time.Time) {
		if col == nil {
			return
		}
		f.latency.Observe(float64(time.Since(begin).Nanoseconds()/1e3), map[string]string{`collections`: col.(string)})
	}(time.Now())

	mapCol := make(map[string]interface{})
	readyToDelete := false
	if rec.Value() == nil {
		val := f.getState(rec.Key())
		if val != nil {
			rec = val.(connector.Recode)
			readyToDelete = true
		}
	}
	// if still previous state was empty and the first value is null cant sync
	if rec.Value() == nil {
		return fmt.Errorf(fmt.Sprintf("could sync the payload of null, no previous states to be mapped:, key: %v, value: %v", rec.Key(), rec.Value()))
	}
	err = json.Unmarshal([]byte(rec.Value().(string)), &mapCol)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("could not create the payload: %v, key: %v, value: %v", err, rec.Key(), rec.Value()))
	}

	paths := strings.Split(col.(string), "/")

	pkCol := f.get(fmt.Sprintf(`pk/%s`, col.(string)))
	// replace col path template from payload path
	col = f.getCollPath(paths, rec.Value().(string))

	// replace primary key for the template
	if strings.Contains(col.(string), "${pk}") {
		col = strings.Replace(col.(string), "${pk}", rec.Key().(string), -1)
	}

	colRef, docRef := f.getPathRefs(paths)

	// if pk available or not
	if len(paths)%2 == 0 {
		if docRef == nil {
			return fmt.Errorf("could not create firestore col for: %v", col)
		}
		_, err = docRef.Set(ctx, mapCol)

		if err != nil {
			return fmt.Errorf(fmt.Sprintf("could not store to firestore: %v, key: %v, value: %v", err, rec.Key(), rec.Value()))
		}

		// ready to delete
		if readyToDelete && f.get(deleteOnNull).(bool) {
			_, err := docRef.Delete(ctx)
			if err != nil {
				return fmt.Errorf(fmt.Sprintf("could not delete from firestore: %v, key: %v, value: %v", err, rec.Key(), rec.Value()))
			}
			f.log.Debug(fireStoreLogPrefix, fmt.Sprintf("firestore message delete done: %v, key: %v, value: %v", err, rec.Key(), rec.Value()))
		}
		return nil
	}

	// if pk available
	if pkCol != nil {
		_, err = colRef.Doc(fmt.Sprintf("%v", rec.Key())).Set(ctx, mapCol)

		if err != nil {
			return fmt.Errorf(fmt.Sprintf("could not store to firestore: %v, key: %v, value: %v", err, rec.Key(), rec.Value()))
		}
		return nil
	}

	//if pk not available
	_, err = colRef.NewDoc().Create(ctx, mapCol)

	if err != nil {
		return fmt.Errorf(fmt.Sprintf("could not store to firestore: %v, key: %v, value: %v", err, rec.Key(), rec.Value()))
	}
	f.log.Debug(fireStoreLogPrefix, fmt.Sprintf("firestore message insert done: %v, key: %v, value: %v", err, rec.Key(), rec.Value()))
	return nil
}

// getCollPath function is used to construct collection path via payload mapping "col1/${id}/col2/${name}" -> "col1/111/col2/foo"
func (f *task) getCollPath(paths []string, value string) string {
	for i, v := range paths {
		if strings.Contains(v, `$`) {
			paths[i] = replacer.Replace(v)
			paths[i] = gjson.Get(value, paths[i]).String()
		}
	}
	return strings.Join(paths, "/")
}

// getPathRefs function is used create path reference from given "col1/doc1/col2/doc2" format
func (f *task) getPathRefs(paths []string) (colRef *firestore.CollectionRef, docRef *firestore.DocumentRef) {
	for i, p := range paths {
		if i%2 == 0 {
			if colRef == nil {
				colRef = f.client.Collection(p)
				continue
			}
			if docRef != nil {
				colRef = docRef.Collection(p)
				continue
			}
			continue
		}
		docRef = colRef.Doc(p)
	}
	return
}

func (f *task) Name() string {
	return `firestore-sink-task`
}
