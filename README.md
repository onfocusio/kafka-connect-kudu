# Kafka Connect Kudu connector

kafka-connect-kudu is a [Kafka Connector](http://kafka.apache.org/documentation.html#connect)
for loading data to [Apache Kudu](http://kudu.apache.org).

An exhaustive configuration example is available in the `conf/` directory.

# Release notes

**0.3.0**

* Upgraded kudu-client dependency from 1.1.0 to 1.2.0.
* New settings `kudu.operation.timeout.ms` and `kudu.socket.read.timeout.ms`.

**0.2.0**

* New optional property `kudu.table.field`: name of the SinkRecord field giving the name of the target Kudu table.  

**0.1.0**

* The Kudu table must have the same name as the input topic.
* Upserts only.
* Auto-flush mode, for hyperfast operations.
* Retries only on `org.apache.kudu.client.PleaseThrottleException`.
* Configurable number of Kudu client workers.
* Ignore a record field if no column exist in the target table with the same name.
* Run the JAR to see all available configuration properties.

# Alternatives

* [Kafka Connect Kudu](http://docs.datamountaineer.com/en/latest/kudu.html) from @datamountaineer: provides topic routing via own query language (KCQL), a retry mechanism, auto table creation and alteration.
 
# Development

To build a development version you'll need a recent version of Kafka. You can build
kafka-connect-kudu with gradle using `gradle assemble`.

# Contribute

- Source Code: https://github.com/onfocusio/kafka-connect-kudu
- Issue Tracker: https://github.com/onfocusio/kafka-connect-kudu/issues

# License

This Kudu Connector for Kafka Connect is made available under the terms of the Apache License, Version 2, as stated in the file LICENSE.

Individual files may be made available under their own specific license, all compatible with Apache License, Version 2. Please see individual files for details.