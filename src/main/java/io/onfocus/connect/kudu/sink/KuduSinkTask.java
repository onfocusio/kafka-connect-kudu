/*
 * Copyright 2016 Onfocus SAS
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.onfocus.connect.kudu.sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class KuduSinkTask extends SinkTask {
  private static final Logger log = LoggerFactory.getLogger(KuduSinkTask.class);

  KuduSinkConfig config;
  KuduWriter writer;

  /**
   * Get the version of this task. Usually this should be the same as the corresponding {@link org.apache.kafka.connect.connector.Connector} class's version.
   *
   * @return the version, formatted as a String
   */
  @Override
  public String version() {
    return getClass().getPackage().getImplementationVersion();
  }

  /**
   * Start the Task. This should handle any configuration parsing and one-time setup of the task.
   *
   * @param props initial configuration
   */
  @Override
  public void start(Map<String, String> props) {
    log.info("Starting task");
    config = new KuduSinkConfig(props);

    log.info("Connecting to Kudu Master at {}", config.kuduMaster);
    KuduClient.KuduClientBuilder clientBuilder = new KuduClient.KuduClientBuilder(config.kuduMaster);
    if (config.kuduWorkerCount != -1) {
      clientBuilder.workerCount(config.kuduWorkerCount);
    }
    final KuduClient client = clientBuilder.build();
    writer = new KuduWriter(config, client);
  }

  /**
   * Put the records in the sink. Usually this should send the records to the sink asynchronously
   * and immediately return.
   * <p>
   * If this operation fails, the SinkTask may throw a {@link RetriableException} to
   * indicate that the framework should attempt to retry the same call again. Other exceptions will cause the task to
   * be stopped immediately. {@link org.apache.kafka.connect.sink.SinkTaskContext#timeout(long)} can be used to set the maximum time before the
   * batch will be retried.
   *
   * @param records the set of records to send
   */
  @Override
  public void put(Collection<SinkRecord> records) {
    if (records.isEmpty()) { return; }
    final SinkRecord first = records.iterator().next();
    final int recordsCount = records.size();
    log.trace("Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to Kudu...",
      recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset());
    try {
      writer.write(records);
    } catch (KuduException ke) {
      if (ke instanceof PleaseThrottleException) {
        log.warn("Write of {} records failed. Kudu asks to throttle. Will retry.", records.size(), ke);
        throw new RetriableException(ke);
      } else {
        log.warn("Write of {} records failed", records.size(), ke);
        throw new ConnectException(ke);
      }
    }
  }

  /**
   * Flush all records that have been {@link #put} for the specified topic-partitions. The
   * offsets are provided for convenience, but could also be determined by tracking all offsets
   * included in the SinkRecords passed to {@link #put}.
   *
   * @param offsets mapping of TopicPartition to committed offset
   */
  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> offsets) {
    // Not necessary
  }

  /**
   * Perform any cleanup to stop this task. In SinkTasks, this method is invoked only once outstanding calls to other
   * methods have completed (e.g., {@link #put(Collection)} has returned) and a final {@link #flush(Map)} and offset
   * commit has completed. Implementations of this method should only need to perform final cleanup operations, such
   * as closing network connections to the sink system.
   */
  @Override
  public void stop() {
    log.info("Stopping task");
    if (writer != null) writer.close();
  }
}
