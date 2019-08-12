FROM golang:1.12.7

# kafka bootstrap servers eg: localhost:9092,localhost:9093,localhost:9094
ENV BOOTSTRAP_SERVERS localhost:9092
# worker REST endpoint
ENV HOST localhost:8888
# worker group id in kafka
ENV CLUSTER_ID kafka-connect-cluster-1

# worker config storage topics
ENV TOPICS_CONNECTOR_CONFIGS stream-connect-config
ENV TOPICS_CONNECTOR_STATUS stream-connect-status

# worker log settings
ENV LOGGER_LEVEL TRACE
ENV LOGGER_COLORS true
ENV LOGGER_FILEPATH true

# connector metrics settings
ENV METRICS_ENABLED false
ENV METRICS_NAMESPACE mybudget_cdc
ENV METRICS_SUBSYSTEM connect_worker
ENV METRICS_HOST $HOST

ENV GO111MODULE=on

RUN apt-get update -y && apt-get install git -y

# get connector with go get
WORKDIR /go/src/bitbucket.org/mybudget-dev/stream-connect-worker
RUN go get bitbucket.org/mybudget-dev/stream-connect-worker@develop

# build connector
RUN cd /go/pkg/mod/bitbucket.org/mybudget-dev/*/main && go build -v main.go && cp ./main /opt && cp ./config.yaml /opt

# build firestore plugin
WORKDIR /opt/tmp
RUN git clone https://github.com/noelyahan/kafka-connect-firestore.git && cd kafka-connect-firestore && git checkout connecter-interface-updates && ls
WORKDIR /opt/tmp/kafka-connect-firestore
RUN go build -v -buildmode=plugin
RUN mkdir /plugins && mv kafka-connect-firestore.so /plugins/firestore-sink.so
RUN cd /plugins && echo "Available plugins" \n "===================================" && ls && echo "=========================================================="


WORKDIR /opt
EXPOSE 8888
ENTRYPOINT "./main"