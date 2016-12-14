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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kudu.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class KuduWriter {
  private static final Logger log = LoggerFactory.getLogger(KuduWriter.class);

  private final KuduSinkConfig config;
  private final KuduClient client;
  private final KuduSession session;

  final Map<String, KuduTable> kuduTables = new HashMap<>();

  public KuduWriter(final KuduSinkConfig config, final KuduClient client) {
    this.config = config;
    this.client = client;

    this.session = client.newSession();

    // Ignore duplicates in case of redelivery
    session.setIgnoreAllDuplicateRows(true);

    // Let the client handle flushes
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
  }

  /**
   * Convert SinkRecord
   *
   * @param records
   * @throws KuduException
   */
  void write(final Collection<SinkRecord> records) throws KuduException {
    for (SinkRecord record : records) {
      final KuduTable table = destinationTable(record);
      final Upsert upsert = table.newUpsert();
      final PartialRow row = upsert.getRow();

      final List<Field> fields = record.valueSchema().fields();
      final Set<String> kuduColNames = table.getSchema().getColumns().stream().map((c) -> c.getName()).collect(Collectors.toSet());
      for (Field field : fields) {
        if (kuduColNames.contains(field.name())) { // Ignore fields missing from the table
          addFieldToRow(record, field, row);
        }
      }

      // Since we're in auto-flush mode, this will return immediately
      session.apply(upsert);
    }

    flush();
  }

  /**
   * Open or reuse a {@link KuduTable} macthing the record topic.
   *
   * @param record
   * @return
   * @throws KuduException
   */
  KuduTable destinationTable(SinkRecord record) throws KuduException {
    final String tableName = record.topic();
    KuduTable table = kuduTables.get(tableName);
    if (table == null) {
      table = client.openTable(tableName);
      kuduTables.put(tableName, table);
    }
    return table;
  }

  /**
   * Force the session to flush its buffers.
   * */
  public void flush() throws KuduException {
    if (session != null && !session.isClosed()) {
      final List<OperationResponse> responses = session.flush();
      for (OperationResponse response : responses) {
        if (response.hasRowError()) {
          // TODO would there be a reason to throw RetriableException ?
          throw new ConnectException(response.getRowError().toString());
        }
      }
    }
  }

  /**
   * Close {@link KuduSession} and {@link KuduClient}.
   * */
  public void close() {
    if (session != null && !session.isClosed()) {
      try {
        session.close();
      } catch (KuduException ke) {
        log.warn("Error closing the Kudu session: {}", ke.getMessage());
      }
    }
    if (client != null) {
      try {
        client.shutdown();
      } catch (KuduException ke) {
        log.warn("Error closing the Kudu client: {}", ke.getMessage());
      }
    }
  }

  /**
   * Convert a {@link SinkRecord} type to Kudu and add the column to the Kudu {@link PartialRow}.
   *
   * @param field SinkRecord Field
   * @param record Sink record
   * @param row The Kudu row to add the field to
   * @return the updated Kudu row
   **/
  private PartialRow addFieldToRow(SinkRecord record, Field field, PartialRow row) {
    Schema.Type fieldType = field.schema().type();
    String fieldName = field.name();
    Struct struct = (Struct)record.value();

    switch (fieldType) {
      case STRING:
        row.addString(fieldName, struct.getString(fieldName));
        break;
      case INT8:
        row.addByte(fieldName, struct.getInt8(fieldName));
        break;
      case INT16:
        row.addShort(fieldName, struct.getInt16(fieldName));
        break;
      case INT32:
        row.addInt(fieldName, struct.getInt32(fieldName));
        break;
      case INT64:
        row.addLong(fieldName, struct.getInt64(fieldName));
        break;
      case BOOLEAN:
        row.addBoolean(fieldName, struct.getBoolean(fieldName));
        break;
      case FLOAT32:
        row.addFloat(fieldName, struct.getFloat32(fieldName));
        break;
      case FLOAT64:
        row.addFloat(fieldName, struct.getFloat64(fieldName).floatValue());
        break;
      case BYTES:
        row.addBinary(fieldName, struct.getBytes(fieldName));
        break;
      default:
        throw new ConnectException("Unsupported source data type: " + fieldType);
    }

    return row;
  }

}
