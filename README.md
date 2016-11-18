flume-ng-druid-sink
===================

A Flume NG sink for Druid.io, using the tranquility API.


Current versions supported
--------------------------
- Tested using Apache Flume NG 1.6.0
- Tested using Druid.io 0.9.1.1 and Tranquility API 2.11


Compilation and packaging
-------------------------
```
  $ mvn package
```


Deployment
----------

Copy flume-ng-druid-sink-<version>.jar in target folder into flume plugins dir folder
```
  $ mkdir -p $FLUME_HOME/plugins.d/druid-sink/lib $FLUME_HOME/plugins.d/druid-sink/libext
  $ cp flume-ng-druid-sink-0.0.1.jar $FLUME_HOME/plugins.d/druid-sink/lib
```

### Specific libraries

##### Flume NG 1.6.0
Download the Netty 3.10.1-Final jar and place it in the Flume NG 1.6.0 lib directory:
```
$ wget http://central.maven.org/maven2/io/netty/netty/3.10.1.Final/netty-3.10.1.Final.jar
$ rm $FLUME_HOME/lib/netty-3.5.12.Final.jar
$ mv netty-3.10.1.Final.jar $FLUME_HOME/lib
```

Download the Scala Library 2.11.6 jar and place it in the Flume NG 1.6.0 lib directory:
```
$ wget http://central.maven.org/maven2/org/scala-lang/scala-library/2.11.6/scala-library-2.11.6.jar
$ rm $FLUME_HOME/lib/scala-library-2.10.1.jar
$ mv scala-library-2.11.6.jar $FLUME_HOME/lib
```


Configuration of Druid.io Sink
------------------------------
Mandatory properties in <b>bold</b>

| Property Name | Default | Type | Description |
| --------------| ------- | ---- | ----------- |
| <b>indexService</b> | - | String | Overlord's service name
| <b>discoveryPath</b> | - | String | Your overlord's druid.discovery.curator.path
| <b>dimensions</b> | - | String | Comma separated list with event headers you want to stored. Similar to columns in relational databases. 
| firehosePattern | druid&#58;firehose&#58;%s | String |  Firehoses describe the data stream source. Make up a service pattern, include %s somewhere in it. This will be used for internal service-discovery purposes, to help druid sink find Druid indexing tasks.
| dataSource | sampleSource | String | Source name where events will be stored. Very similar to a table in relational databases.
| aggregators | - | String (json format) | Different specifications of processing over available metrics (COUNT, DOUBLESUM, LONGSUM, DOUBLEMAX, LONGMAX, DOUBLEMIN, LONGMIN and HYPERUNIQUES).
| zookeeperLocation | 127.0.0.1&#58;2181 | String | Zookeeper location (hostname:port).
| timestampField | timestamp | String | The field name where event timestamp info is extracted from.
| segmentGranularity | HOUR | Granularity | Time granularity (minute, hour, day, week, month) for loading data at query time. Recommended, more than queryGranularity.
| queryGranularity | NONE | Granularity | Time granularity (minute, hour, day, week, month) for rollup. At least, less than segmentGranularity. Recommended: minute, hour, day, week, month.
| windowPeriod | PT10M | Period | While reading, events with timestamp older than now minus this value, will be discarded.
| partitions | 1 | Integer | This is used to scale ingestion up to handle larger streams.
| replicants | 1 | Integer | This is used to provide higher availability and parallelism for queries.
| baseSleepTime | 1000 | Integer | Initial amount of time to wait between retries.
| maxRetries | 3 | Integer | Max number of times to retry.
| maxSleep | 30000 | Integer | Max time in ms to sleep on each retry.
| batchSize | 1000 | Integer | Number of events to batch together to be send to our data source.


Configuration example
---------------------

```properties

#
agent.sinks = druidSink

# Describe the Druid sink
agent.sinks.druidSink.type = com.konkerlabs.analytics.ingestion.sink.TranquilitySink

agent.sinks.druidSink.indexService = druid/overlord
agent.sinks.druidSink.discoveryPath = /druid/discovery
agent.sinks.druidSink.dimensions = field0
agent.sinks.druidSink.dataSource = testDS
agent.sinks.druidSink.aggregators = {"DOUBLESUM":["field1","field2","field3"],"LONGSUM":["field4","field5","field6"],"DOUBLEMAX":["field7"],"COUNT":["field8","field9"]}
agent.sinks.druidSink.timestampField = timestamp
agent.sinks.druidSink.timestampFormat = YYYY-MM-dd HH:mm:ss.SSS
agent.sinks.druidSink.segmentGranularity = HOUR
agent.sinks.druidSink.queryGranularity = HOUR
agent.sinks.druidSink.zookeeperLocation = 127.0.0.1:2181
agent.sinks.druidSink.firehosePattern = druid:firehose:%s
agent.sinks.druidSink.windowPeriod = PT10M
agent.sinks.druidSink.partitions = 1
agent.sinks.druidSink.replicants = 1
agent.sinks.druidSink.baseSleepTime = 1000
agent.sinks.druidSink.maxRetries = 3
agent.sinks.druidSink.maxSleep = 10000
agent.sinks.druidSink.batchSize = 100
```


Recommendation
--------------
The normal, expected use cases have the following overall constraints: queryGranularity < windowPeriod < segmentGranularity.


Troubles
--------
Long granularities may cause very long delay in Druid's flush to disk.


Special thanks
--------------
Thanks to [KonkerLabs](http://www.konkerlabs.com/) team!