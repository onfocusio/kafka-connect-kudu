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
    // "The writes will be sent in the background, potentially batched together with other writes from
    // the same session. If there is not sufficient buffer space, then
    // {@link KuduSession#apply KuduSession.apply()} may block for buffer space to be available."
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
      if (table == null) continue;
      final Upsert upsert = table.newUpsert();
      final PartialRow row = upsert.getRow();

      // Get column names of the Kudu table
      final Set<String> kuduColNames = table.getSchema().getColumns().stream().map((c) -> c.getName()).collect(Collectors.toSet());

      // Set the fields of the record value to a new Kudu row
      Struct value = (Struct)record.value();
      for (Field field : record.valueSchema().fields()) {
        Schema.Type fieldType = field.schema().type();
        // Check if the field name is in the Kudu table. If it's an array, the field name will be generated then checked later.
        if (fieldType.equals(Schema.Type.ARRAY) || kuduColNames.contains(field.name())) {
          addStructToRow(value, field, fieldType, row, kuduColNames);
        }
      }

      // Add the fields of the record key to the Kudu row
      if (config.kuduKeyInsert) {
        Struct key = (Struct)record.key();
        for (Field field : record.keySchema().fields()) {
          Schema.Type fieldType = field.schema().type();
          // Check if the field name is in the Kudu table. If it's an array, the field name will be generated then checked later.
          if (fieldType.equals(Schema.Type.ARRAY) || kuduColNames.contains(field.name())) {
            addStructToRow(key, field, fieldType, row, kuduColNames);
          }
        }
      }

      // Since we're in auto-flush mode, this will return immediately
      session.apply(upsert);
    }

    flush();
  }

  /**
   * Open or reuse a {@link KuduTable} macthing the record or the value
   * of the record field configured with `kudu.table.field`.
   *
   * @param record
   * @return
   * @throws KuduException
   */
  KuduTable destinationTable(SinkRecord record) throws KuduException {
    String tableName;
    if (config.kuduTableField != null) {
      tableName = ((Struct)record.value()).getString(config.kuduTableField);
      if (config.kuduTableFilter != null && tableName.indexOf(config.kuduTableFilter) != -1) {
        return null;
      }
    } else {
      tableName = record.topic();
    }
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
   * @param struct Struct value
   * @param field SinkRecord Field
   * @paran fieldType
   * @param row The Kudu row to add the field to
   * @return the updated Kudu row
   **/
  private PartialRow addStructToRow(Struct struct, Field field, Schema.Type fieldType, PartialRow row, Set<String> kuduColNames) {
    String fieldName = field.name();

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
        row.addDouble(fieldName, struct.getFloat64(fieldName));
        break;
      case BYTES:
        row.addBinary(fieldName, struct.getBytes(fieldName));
        break;
      case ARRAY:
        // Support for arrays is handled by adding an index suffix to the field name, starting with "_1".
        ListIterator<Object> innerValues = struct.getArray(fieldName).listIterator();
        Schema.Type innerFieldsType = field.schema().valueSchema().type();
        switch (innerFieldsType) {
          case STRING:
            while (innerValues.hasNext()) {
              String finalFieldName = fieldName + "_" + (innerValues.nextIndex()+1);
              if (kuduColNames.contains(finalFieldName))
                row.addString(finalFieldName, (String) innerValues.next());
              else
                innerValues.next(); // Consume the iterator
            }
            break;
          case INT8:
            while (innerValues.hasNext()) {
              String finalFieldName = fieldName + "_" + (innerValues.nextIndex()+1);
              if (kuduColNames.contains(finalFieldName))
                row.addByte(finalFieldName, (Byte) innerValues.next());
              else
                innerValues.next(); // Consume the iterator
            }
            break;
          case INT16:
            while (innerValues.hasNext()) {
              String finalFieldName = fieldName + "_" + (innerValues.nextIndex()+1);
              if (kuduColNames.contains(finalFieldName))
                row.addShort(finalFieldName, (Short) innerValues.next());
              else
                innerValues.next(); // Consume the iterator
            }
            break;
          case INT32:
            while (innerValues.hasNext()) {
              String finalFieldName = fieldName + "_" + (innerValues.nextIndex()+1);
              if (kuduColNames.contains(finalFieldName))
                row.addInt(finalFieldName, (Integer) innerValues.next());
              else
                innerValues.next(); // Consume the iterator
            }
            break;
          case INT64:
            while (innerValues.hasNext()) {
              String finalFieldName = fieldName + "_" + (innerValues.nextIndex()+1);
              if (kuduColNames.contains(finalFieldName))
                row.addLong(finalFieldName, (Long) innerValues.next());
              else
                innerValues.next(); // Consume the iterator
            }
            break;
          case BOOLEAN:
            while (innerValues.hasNext()) {
              String finalFieldName = fieldName + "_" + (innerValues.nextIndex()+1);
              if (kuduColNames.contains(finalFieldName))
                row.addBoolean(finalFieldName, (Boolean) innerValues.next());
              else
                innerValues.next(); // Consume the iterator
            }
            break;
          case FLOAT32:
            while (innerValues.hasNext()) {
              String finalFieldName = fieldName + "_" + (innerValues.nextIndex()+1);
              if (kuduColNames.contains(finalFieldName))
                row.addFloat(finalFieldName, (Float) innerValues.next());
              else
                innerValues.next(); // Consume the iterator
            }
            break;
          case FLOAT64:
            while (innerValues.hasNext()) {
              String finalFieldName = fieldName + "_" + (innerValues.nextIndex()+1);
              if (kuduColNames.contains(finalFieldName))
                row.addDouble(finalFieldName, (Double) innerValues.next());
              else
                innerValues.next(); // Consume the iterator
            }
            break;
          case BYTES:
            while (innerValues.hasNext()) {
              String finalFieldName = fieldName + "_" + (innerValues.nextIndex()+1);
              if (kuduColNames.contains(finalFieldName))
                row.addBinary(finalFieldName, (byte[]) innerValues.next());
              else
                innerValues.next(); // Consume the iterator
            }
            break;
          default:
            throw new ConnectException("Unsupported source data type in array field '" + fieldName + "': " + fieldType);
        }
        break;
      default:
        throw new ConnectException("Unsupported source data type: " + fieldType);
    }

    return row;
  }
}
