# Wookiee - Component: Kafka

[![Build Status](https://travis-ci.org/oracle/wookiee-kafka.svg?branch=master)](https://travis-ci.org/oracle/wookiee-kafka) [![Latest Release](https://img.shields.io/github/release/oracle/wookiee-kafka.svg)](https://github.com/oracle/wookiee-kafka/releases) [![License](http://img.shields.io/:license-Apache%202-red.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

[Main Wookiee Project](https://github.com/oracle/wookiee)

For Configuration information see [Kafka Config](docs/config.md)

The kafka component allows users to set up a Worker which can be replicated and consumes from a Kafka topic. It is also capable of coordinating multiple other Nodes (via Zookeeper) to share the load of topic consumption. In other words, this component allows one to create a horizontally scalable kafka consumer app with ease.

### Adding to Pom

Add the jfrog repo to your project first:
~~~~
<repositories>
    <repository>
        <id>JFrog</id>
        <url>http://oss.jfrog.org/oss-release-local</url>
    </repository>
</repositories>
~~~~

Add [latest version](https://github.com/oracle/wookiee-zookeeper/releases/latest) of wookiee:
~~~~
<dependency>
    <groupId>com.webtrends</groupId>
    <artifactId>wookiee-kafka_2.11</artifactId>
    <version>${wookiee.version}</version>
</dependency>
~~~~

### Contributing
This project is not accepting external contributions at this time. For bugs or enhancement requests, please file a GitHub issue unless it’s security related. When filing a bug remember that the better written the bug is, the more likely it is to be fixed. If you think you’ve found a security vulnerability, do not raise a GitHub issue and follow the instructions in our [security policy](./SECURITY.md).

### PartitionConsumerWorker

This is the most important class and will have to be overridden, create a class of your own that extends it within your app. Then override the function:
```
    def onReceive(messageResponse: MessageResponse) {}
```
This method will actually handle the event consumed from Kafka and process it in the way your app sees fit.

### Configuration

# Base
Base configuration is simple, when not using a producer or consumer all one needs to provide is an app-name
```json
wookiee-kafka {
    app-name = "test"
}
```

# Consumer
If one would like to utilize horizontally scalable consumers, then set these properties and
override the onReceive(messageResponse: MessageResponse) method in PartitionConsumerWorker.scala
```json
wookiee-kafka {
    app-name = "test"
    cluster-id = "collection" // Will be used to build up zookeeper path
    worker-class = "com.product.code.CustomWorker"
    zk-offset-commit-rate-millis = 500
    consumer {
      topics = [
        {
          name = "Lab_G_scsRawHits"
          event-age-threshold-seconds = 90
        },
        {
          name = "Lab_G_dcRawHits"
          event-age-threshold-seconds = 0
        }
      ]

      kafka-hosts = [
        {
          "id": "cluster1"
          "brokers": ["server1.com","server2.com"]
        },
        {
          "id": "cluster2"
          "brokers": ["2server1.com"]
        }
    ]
}
```

One will also need to pull in the wookiee-zookeeper component and configure it like so
```json
wookiee-zookeeper {
  datacenter = "Lab"
  pod = "Tests"
  quorum = "zoo01.keeper.org"
  session-timeout = 30s
  connection-timeout = 30s
  retry-sleep = 5s
  retry-count = 150
  base-path = "/discovery/clusters"

  message-processor {
    # How often the MessageProcessor should share it's subscription information
    share-interval = 1s
    # When should MessageTopicProcessor instances be removed after there are no longer any subscribers for that topic
    trash-interval = 30s
    # The default send timeout
    default-send-timeout = 2s
  }
}
```

# Producer
If one would like to write to a set of Kafka brokers then configure the producers like so
```json
wookiee-kafka {
    app-name = "test"

    producer {
      producer.type="sync"
      metadata.broker.list="broker1.com:9092,broker2.com:9092"
      request.required.acks=1
      queue.time=5000
      queue.size=10000
      batch.size=200
      compression.codec="gzip"
    }
}
```

## License
Copyright (c) 2004 Oracle and/or its affiliates.
Released under the Apache License Version 2.0
