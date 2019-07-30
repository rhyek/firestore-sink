# Kafka Connect FireStore

kafka-connect-firestore is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect)
for loading data to and from Kafka.

# Development

To build a development version you'll need a correct github.com/gmbyapa/kafka-connector worker version.

# Configurations
``` "topics": "topic1, topic2" ```<br />
``` "firestore.credentials.file.path": "file_path" ```<br />
``` "firestore.credentials.file.json": "json cred details" ```<br />
``` "firestore.project.id": "app_1" ```<br />
``` "firestore.collection": "col1" ```<br />
``` "firestore.collections": "col1,col2" ```<br />

Topic collection mapping<br>
``` "firestore.collections.col1": "topic1" ```<br />
``` "firestore.collections.col2": "topic2" ```<br />


If collection wants to use topic primary key as the collection PK<br>
``` "firestore.topic.pk.collections": "col1,col2" ```<br />

Sample Connector Payload
```
{
  "name": "fire_store_sink",
  "task.max": 1,
  "plugin.path": "/opt/kafka-connect-firestore.so",
  "configs": {
      "log.level": "TRACE",
      "log.colors": "true",
      "consumer.bootstrap.servers": "192.168.10.60:9092",
      "encoding.key": "string",
      "encoding.value": "json",
      "topics": "fire-store-source-topic1",
      "transforms": "Cast,InsertField,ReplaceField",
      "transforms.Cast.type": "Cast$Value",
      "transforms.Cast.spec": "followers:string,public_repos:string",
"transforms.InsertField.type": "InsertField$Value",
"transforms.InsertField.static.field": "age",
"transforms.InsertField.static.value": 100,
"transforms.ReplaceField.type": "ReplaceField$Value",
"transforms.ReplaceField.whitelist": "name,company,blog,location,email,followers,public_repos,age",
"transforms.ReplaceField.renames": "name:user.name,company:other.company,blog:user.blog,location:other.location,email:user.email,followers:other.followers,public_repos:user.repository_count,age:user.Age",
"firestore.collection.fire-store-source-topic1": "github",
      "firestore.project.id": "test-budget-5529f",
      "firestore.credentials.file.json": {
"type": "service_account",
"project_id": "test-budget-123",
"private_key_id": "xxxxxx9be0547",
"private_key": "-----BEGIN PRIVATE KEY-----\xxxx=\n-----END PRIVATE KEY-----\n",
"client_email": "xxxxx.com",
"client_id": "xxxxx",
"auth_uri": "https://accounts.google.com/o/oauth2/auth",
"token_uri": "https://oauth2.googleapis.com/token",
"auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
"client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-efplw%40test-budget-5529f.iam.gserviceaccount.com"
}

  }
}

```