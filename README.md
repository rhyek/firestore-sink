# Kafka Connect FireStore

kafka-connect-firestore is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect)
for loading data to and from Kafka.

# Development

To build a development version you'll need a correct github.com/gmbyapa/kafka-connector worker version.

# Configurations
``` "topics": "topic1, topic2" ```<br/>
``` "firestore.credentials.file.path": "file_path" ```<br/>
``` "firestore.credentials.file.json": "json cred details" ```<br/>
``` "firestore.project.id": "app_1" ```<br/>
``` "firestore.collection": "col1" ```<br/>
``` "firestore.collections": "col1,col2" ```<br/>

Topic collection mapping<br>
``` "firestore.collections.col1": "topic1" ```<br/>
``` "firestore.collections.col2": "topic2" ```<br/>


If collection wants to use topic primary key as the collection PK<br>
``` "firestore.topic.pk.collections": "col1,col2" ```<br/>