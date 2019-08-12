# Kafka Connect FireStore

kafka-connect-firestore is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect)
for loading data to and from Kafka.

# Development

To build a development version you'll need a correct github.com/gmbyapa/kafka-connector worker version.

Please follow below instructions to generate a firestore credentials
![srcImage](doc/firestore-cred-gen.png)

* Visit https://console.developers.google.com
* Select your firestore application
* Select `Credentials` from side bar selection
* Select `Service account key` under `Create Credentials` section and in new tab follow step `5` & `6`.
* Copy the JSON payload and paste on ``` "firestore.credentials.file.json": "json cred details" ``` config

# Configurations
* ``` "topics": "topic1, topic2" ```
* ``` "firestore.credentials.file.path": "file_path" ```
* ``` "firestore.credentials.file.json": "json cred details" ```
* ``` "firestore.project.id": "app_1" ```

Topic collection mapping

* ``` "firestore.collections.topic1": "col1" ```
* ``` "firestore.collections.topic2": "col2" ```
* ``` "firestore.collections.topic3": "col1/col2/col3" ```

Sub topic with template strings

* ``` "firestore.collections.topic1": "col1/${json_payload_field}/col2/${json_payload_field}" ```
* ``` "firestore.collections.topic1": "col1/${json_payload_field}/col2/${pk}" ```


If collection wants to use topic primary key as the collection PK

* ``` "firestore.topic.pk.collections": "col1,col2,col1.col2.col3" ```

## Example

### Sample Connector Payload
```
{
  "name": "fire_store_sink",
  "task.max": 1,
  "plugin.path": "/plugins/firestore-sink.so",
  "configs": {
      "log.level": "TRACE",
      "log.colors": true,
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

### Input JSON

```
{​
    "login": "octocat", ​
    "id": 1, ​
    "node_id": "MDQ6VXNlcjE=", ​
    "avatar_url": "https://github.com/images/error/octocat_happy.gif", ​
    "gravatar_id": "", ​
    "url": "https://api.github.com/users/octocat", ​
    "html_url": "https://github.com/octocat", ​
    "followers_url": "https://api.github.com/users/octocat/followers", ​
    "following_url": "https://api.github.com/users/octocat/following{/other_user}", ​
    "gists_url": "https://api.github.com/users/octocat/gists{/gist_id}", ​
    "starred_url": "https://api.github.com/users/octocat/starred{/owner}{/repo}", ​
    "subscriptions_url": "https://api.github.com/users/octocat/subscriptions", ​
    "organizations_url": "https://api.github.com/users/octocat/orgs", ​
    "repos_url": "https://api.github.com/users/octocat/repos", ​
    "events_url": "https://api.github.com/users/octocat/events{/privacy}", ​
    "received_events_url": "https://api.github.com/users/octocat/received_events", ​
    "type": "User", ​
    "site_admin": false, ​
    "name": "monalisa octocat", ​
    "company": "GitHub", ​
    "blog": "https://github.com/blog", ​
    "location": "San Francisco", ​
    "email": "octocat@github.com", ​
    "hireable": false, ​
    "bio": "There once was...", ​
    "public_repos": 2, ​
    "public_gists": 1, ​
    "followers": 20, ​
    "following": 0, ​
    "created_at": "2008-01-14T04:33:35Z", ​
    "updated_at": "2008-01-14T04:33:35Z"​
}​
```

### Transform: Output JSON

```
{​
    "other": {​
        "followers": "20", ​
        "location": "San Francisco", ​
        "company": "GitHub"​
    }, ​
    "user": {​
        "Age": 100, ​
        "repository_count": "2", ​
        "email": "octocat@github.com", ​
        "blog": "https://github.com/blog", ​
        "name": "noel yahan"​
    }​
}​
```