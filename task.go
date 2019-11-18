package main

import (
	"bitbucket.org/mybudget-dev/stream-connect-worker/connector"
	"cloud.google.com/go/firestore"
	"context"
	"encoding/json"
	"fmt"
	"github.com/pickme-go/k-stream/consumer"
	"github.com/pickme-go/k-stream/producer"
	"github.com/pickme-go/log"
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

var replacer = strings.NewReplacer("$", "", "{", "", "}", "")

type record struct {
	Topic_     string      `json:"topic"`
	Partition_ int32       `json:"partition"`
	Offset_    int64       `json:"offset"`
	Key_       interface{} `json:"key"`
	Value_     interface{} `json:"value"`
	Timestamp_ time.Time   `json:"timestamp"`
}

func (r record) Topic() string        { return r.Topic_ }
func (r record) Partition() int32     { return r.Partition_ }
func (r record) Offset() int64        { return r.Offset_ }
func (r record) Key() interface{}     { return r.Key_ }
func (r record) Value() interface{}   { return r.Value_ }
func (r record) Timestamp() time.Time { return r.Timestamp_ }

type task struct {
	log        log.Logger
	config     sync.Map
	state      sync.Map
	client     *firestore.Client
	latency    metrics.Observer
	syncTopic  string
	upSyncDone chan struct{}
	producer   producer.Producer
	consumer   consumer.PartitionConsumer
}

func (f *task) Init(config *connector.TaskConfig) (err error) {
	f.log = config.Logger.NewLog(log.Prefixed(`firestore_sink`))

	// create this hassle only if delete on null is enabled
	defer func() {
		if !f.getConfig(deleteOnNull).(bool) {
			close(f.upSyncDone)
			return
		}
		f.syncTopic = fmt.Sprintf("__%v_%v", f.Name(), config.Connector.Name)
		pCfg := producer.NewConfig()
		cCfg := consumer.NewConsumerConfig()

		bs := config.Connector.Configs[`consumer.bootstrap.servers`]
		if bs != nil {
			servers := strings.Split(bs.(string), ",")
			pCfg.BootstrapServers = servers
			cCfg.BootstrapServers = servers
		}

		f.producer, err = producer.NewProducer(pCfg)
		if err != nil {
			f.log.Error(fmt.Sprintf("could not initiate the connector producer for state sync, please check bootstrap servers: %v", err))
			return
		}

		cCfg.GroupId = f.syncTopic
		cCfg.Consumer.Offsets.Initial = int64(consumer.Earliest)
		f.consumer, err = consumer.NewPartitionConsumer(cCfg)
		if err != nil {
			f.log.Error(fmt.Sprintf("could not initiate the connector consumer for state sync, please check bootstrap servers: %v", err))
			return
		}

		// start previous state sync
		f.consumeStates()
		close(f.upSyncDone)
	}()

	f.validate(config)
	ctx := context.Background()
	var opt option.ClientOption
	credFilePath := f.getConfig(credentialsFilePath)
	credFileJSON := f.getConfig(credentialsFileJson)
	if credFilePath != nil {
		opt = option.WithCredentialsFile(credFilePath.(string))
	} else if credFileJSON != nil {
		b, err := json.Marshal(credFileJSON)
		if err != nil {
			f.log.Error(fmt.Sprintf("could not convert json firestore credentials"))
			return err
		}
		opt = option.WithCredentialsJSON(b)
	}
	if opt == nil {
		opt = option.WithoutAuthentication()
	}
	projectId := f.getConfig(projectId).(string)
	client, err := firestore.NewClient(ctx, projectId, opt)
	if err != nil {
		f.log.Error(fmt.Sprintf("could not connect to firestore, error on creating a client: %v", err))
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
		f.log.Error(fmt.Sprintf("error on closinf the client: %v", err))
		return err
	}
	return nil
}

// OnRebalanced not implemented
func (f *task) OnRebalanced() connector.ReBalanceHandler { return nil }

// Process used to do the synchronization (CRUD) for firestore
func (f *task) Process(records []connector.Recode) error {
	// wait till the init sync is done
	<-f.upSyncDone
	ctx := context.Background()
	for _, rec := range records {
		err := f.store(ctx, rec)
		if err != nil {
			f.log.Error(err, rec.Key(), rec.Value())
			continue
		}
		f.log.Trace(`batch processed records: %+v`, records)
	}
	return nil
}

func (f *task) Name() string {
	return `firestore-sink-task`
}

// getConfig used to getConfig config values
func (f *task) getConfig(key string) interface{} {
	res, _ := f.config.Load(key)
	return res
}

// getConfig used to setConfig config values
func (f *task) setConfig(key string, value interface{}) {
	f.config.Store(key, value)
}

// setState is used to keep local cache of previous status
func (f *task) setState(key, value interface{}) {
	f.state.Store(key, value)
}

// getState is used to getConfig local cache of previous status
func (f *task) getState(key interface{}) interface{} {
	value, _ := f.state.Load(key)
	return value
}

// consumeStates is used to sink previous status from up stream, if connector crash happens or if new connector spawns
func (f *task) consumeStates() {
	messages, err := f.consumer.Consume(f.syncTopic, 0, consumer.Earliest)
	if err != nil {
		f.log.Error(fmt.Sprintf("could not sync data from sinker source: %v", err))
	}

	for message := range messages {
		switch m := message.(type) {
		case *consumer.PartitionEnd:
			f.log.Trace(`done sync latest states from sinker source`)
			return
		case *consumer.Record:
			var key interface{}
			err := json.Unmarshal(m.Key, &key)
			if err != nil {
				f.log.Error(fmt.Sprintf("error on decoding key sinker source: %v", err))
				continue
			}

			var rec record
			err = json.Unmarshal(m.Value, &rec)
			if err != nil {
				f.log.Error(fmt.Sprintf("error on decoding key sinker source: %v", err))
				continue
			}
			f.setState(key, rec)
		}
	}
}

// publishStates is used to sink previous status to up stream
func (f *task) publishStates(ctx context.Context, rec connector.Recode) error {
	if rec == nil {
		return fmt.Errorf("current record is nil")
	}
	r := record{rec.Topic(), rec.Partition(),
		rec.Offset(), rec.Key(), rec.Value(), rec.Timestamp()}

	key, err := json.Marshal(r.Key_)
	if err != nil {
		f.log.Error(fmt.Sprintf("could not encode the json data key: %+v, err: %v", rec, err))
		return err
	}

	value, err := json.Marshal(r)
	if err != nil {
		f.log.Error(fmt.Sprintf("could not encode the json data value: %+v, err: %v", rec, err))
		return err
	}
	if f.producer != nil {
		_, _, err = f.producer.Produce(ctx, &consumer.Record{
			Key:   key,
			Value: value,
			Topic: f.syncTopic,
		})
		if err != nil {
			f.log.Error(fmt.Sprintf("could not produce to sinker source data: %+v, err: %v", rec, err))
		}
	}
	return nil
}

func (f *task) validate(config *connector.TaskConfig) {
	f.upSyncDone = make(chan struct{})
	f.latency = config.Connector.Metrics.Observer(metrics.MetricConf{
		Path:        `firestore_sink_connector_write_latency_microseconds`,
		Labels:      []string{`collections`},
		ConstLabels: map[string]string{`task`: config.TaskId},
	})
	var conf interface{}
	conf = config.Connector.Configs[topics]
	if conf == nil {
		msg := fmt.Sprintf(`%v conifig canot be empty`, topics)
		f.log.Error(msg)
	}
	f.setConfig(topics, conf)

	conf = config.Connector.Configs[credentialsFilePath]
	f.setConfig(credentialsFilePath, conf)

	conf = config.Connector.Configs[credentialsFileJson]
	if conf == nil && config.Connector.Configs[credentialsFilePath] == nil {
		msg := fmt.Sprintf(`%v conifig canot be empty`, credentialsFileJson)
		f.log.Error(msg)
	}
	f.setConfig(credentialsFileJson, conf)

	conf = config.Connector.Configs[projectId]
	if conf == nil {
		msg := fmt.Sprintf(`%v conifig could not be empty`, projectId)
		f.log.Error(msg)
	}
	f.setConfig(projectId, conf)

	// topics and collections mapping - optional
	topics := strings.Split(config.Connector.Configs[topics].(string), ",")

	for _, t := range topics {
		t = strings.Replace(t, " ", "", -1)
		key := fmt.Sprintf(`%v.%v`, `firestore.collection`, t)

		col := config.Connector.Configs[key]
		if col != nil {
			f.setConfig(t, col.(string))
		}
	}

	conf = config.Connector.Configs[deleteOnNull]
	f.setConfig(deleteOnNull, false)
	if conf != nil {
		f.setConfig(deleteOnNull, conf.(bool))
	}
	conf = config.Connector.Configs[pkMode]
	if conf == nil {
		return
	}
	pkCols := strings.Split(conf.(string), ",")
	for _, c := range pkCols {
		f.setConfig(fmt.Sprintf(`pk/%s`, c), struct{}{})
	}
}

// TODO if value is not a JSON, just publish for the given collection if can (Only JSON values are supported)
func (f *task) store(ctx context.Context, rec connector.Recode) error {
	var err error
	defer func(err error) {
		if err != nil {
			return
		}
		if !f.getConfig(deleteOnNull).(bool) {
			return
		}
		// sync state
		f.setState(rec.Key(), rec)
		err = f.publishStates(ctx, rec)
		if err != nil {
			f.log.Error(fmt.Sprintf("could not sync latetst state to sink: %v", err))
		}
	}(err)
	// topic collections mapping info
	col := f.getConfig(rec.Topic())
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
	tmpRec := rec
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
		return fmt.Errorf(fmt.Sprintf("current record is null and no previous states to be mapped the firestore collection path:, key: %+v, value: %+v", rec.Key(), rec.Value()))
	}
	if !isJSON(rec.Value()) {
		err = fmt.Errorf("record value is not a JSON value %+v", rec.Value())
		return err
	}
	err = json.Unmarshal([]byte(rec.Value().(string)), &mapCol)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("could not create the payload: %v, key: %v, value: %v", err, rec.Key(), rec.Value()))
	}

	paths := strings.Split(col.(string), "/")

	pkCol := f.getConfig(fmt.Sprintf(`pk/%s`, col.(string)))
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
			return fmt.Errorf("could not create the firestore col for: %v", col)
		}
		_, err = docRef.Set(ctx, mapCol)
		if err != nil {
			return fmt.Errorf(fmt.Sprintf("could not store to the firestore: %v, key: %+v, value: %+v", err, rec.Key(), rec.Value()))
		}

		// ready to delete
		if readyToDelete && f.getConfig(deleteOnNull).(bool) {
			rec = tmpRec
			_, err := docRef.Delete(ctx)
			if err != nil {
				return fmt.Errorf(fmt.Sprintf("could not delete from the firestore: %v, key: %+v, value: %+v", err, rec.Key(), rec.Value()))
			}
			f.log.Debug(fmt.Sprintf("firestore message delete done: %v, key: %+v, value: %+v", err, rec.Key(), rec.Value()))
		}
		return nil
	}

	// if pk available
	if pkCol != nil {
		if readyToDelete && f.getConfig(deleteOnNull).(bool) {
			docRef = colRef.Doc(rec.Key().(string))
			rec = tmpRec
			_, err := docRef.Delete(ctx)
			if err != nil {
				return fmt.Errorf(fmt.Sprintf("could not delete from the firestore: %v, key: %+v, value: %+v", err, rec.Key(), rec.Value()))
			}
			f.log.Debug(fmt.Sprintf("firestore message delete done: %v, key: %+v, value: %+v", err, rec.Key(), rec.Value()))
			return nil
		}
		_, err = colRef.Doc(fmt.Sprintf("%v", rec.Key())).Set(ctx, mapCol)
		if err != nil {
			return fmt.Errorf(fmt.Sprintf("could not store to the firestore: %v, key: %+v, value: %+v", err, rec.Key(), rec.Value()))
		}
		return nil
	}

	//if pk not available
	_, err = colRef.NewDoc().Create(ctx, mapCol)
	if err != nil {
		return fmt.Errorf(fmt.Sprintf("could not store to the firestore: %v, key: %+v, value: %+v", err, rec.Key(), rec.Value()))
	}
	f.log.Debug(fmt.Sprintf("firestore message insert done: %v, key: %+v, value: %+v", err, rec.Key(), rec.Value()))
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

func isJSON(v interface{}) bool {
	// TODO v is a struct do it in different way
	var jsonStr map[string]interface{}
	s := fmt.Sprintf("%v", v)
	b := []byte(s)
	err := json.Unmarshal(b, &jsonStr)
	return err == nil
}
