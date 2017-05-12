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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kudu.*;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.data.Schema.*;
import static org.mockito.Mockito.*;

public class KuduWriterTest {

  @Mock KuduClient client;
  @Mock KuduSession session;
  @Mock KuduTable table;
  @Mock Upsert upsert;
  @Mock PartialRow row;
  @Mock OperationResponse operationResponse;

  @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

  @Test
  public void testWriteShouldWriteToATableWithTheSameNameAsTheTopic() throws KuduException {
    org.apache.kudu.Schema kuduSchema = new org.apache.kudu.Schema(Arrays.asList(
      new ColumnSchema.ColumnSchemaBuilder("id", Type.STRING).build(),
      new ColumnSchema.ColumnSchemaBuilder("int_field", Type.INT32).build()
    ));
    Schema schema = SchemaBuilder.struct().name("record")
      .version(1)
      .field("id", STRING_SCHEMA)
      .field("int_field", INT32_SCHEMA)
      .build();
    Struct struct = new Struct(schema)
      .put("id", "a")
      .put("int_field", 1);
    SinkRecord record = new SinkRecord("topic", 1, STRING_SCHEMA, "key", struct.schema(), struct, 1);

    when(client.newSession()).thenReturn(session);
    when(session.apply(upsert)).thenReturn(operationResponse);
    when(client.openTable("topic")).thenReturn(table);
    when(table.getSchema()).thenReturn(kuduSchema);
    when(table.newUpsert()).thenReturn(upsert);
    when(upsert.getRow()).thenReturn(row);

    Map<String,String> props = new HashMap<String, String>();
    props.put("kudu.master","0.0.0.0");
    KuduSinkConfig config = new KuduSinkConfig(props);

    KuduWriter writer = new KuduWriter(config, client);

    writer.write(Arrays.asList(record));

    verify(client, times(1)).openTable("topic");
    verify(row, times(1)).addString("id", "a");
    verify(row, times(1)).addInt("int_field", 1);
    verify(session, times(1)).flush();
  }

  @Test
  public void testWriteShouldWriteToATableWithTheSameNameAsTheProperty() throws KuduException {
    org.apache.kudu.Schema kuduSchema = new org.apache.kudu.Schema(Arrays.asList(
      new ColumnSchema.ColumnSchemaBuilder("id", Type.STRING).build()
    ));
    Schema schema = SchemaBuilder.struct().name("record")
      .version(1)
      .field("id", STRING_SCHEMA)
      .field("_table", STRING_SCHEMA)
      .build();

    final String TABLE_1 = "table_1";
    final String TABLE_2 = "table_2";
    final String TABLE_3 = "table_3";
    
    Struct struct1 = new Struct(schema)
      .put("id", "a")
      .put("_table", TABLE_1);
    SinkRecord record1 = new SinkRecord("topic", 1, STRING_SCHEMA, "key", struct1.schema(), struct1, 1);

    Struct struct2 = new Struct(schema)
      .put("id", "a")
      .put("_table", TABLE_2);
    SinkRecord record2 = new SinkRecord("topic", 1, STRING_SCHEMA, "key", struct2.schema(), struct2, 2);

    Struct struct3 = new Struct(schema)
      .put("id", "b")
      .put("_table", TABLE_3);
    SinkRecord record3 = new SinkRecord("topic", 1, STRING_SCHEMA, "key", struct3.schema(), struct3, 3);


    when(client.newSession()).thenReturn(session);
    when(session.apply(upsert)).thenReturn(operationResponse);
    when(client.openTable(TABLE_1)).thenReturn(table);
    when(table.getSchema()).thenReturn(kuduSchema);
    when(table.newUpsert()).thenReturn(upsert);
    when(upsert.getRow()).thenReturn(row);

    Map<String,String> props = new HashMap<String, String>();
    props.put("kudu.master", "0.0.0.0");
    props.put("kudu.table.field", "_table");
    props.put("kudu.table.filter", TABLE_2.substring(TABLE_2.length() - 3, TABLE_2.length()));
    KuduSinkConfig config = new KuduSinkConfig(props);

    KuduWriter writer = new KuduWriter(config, client);

    writer.write(Arrays.asList(record1, record2, record3));

    verify(client, times(1)).openTable(TABLE_1);
    verify(client, never()).openTable(TABLE_2);
    verify(client, times(1)).openTable(TABLE_3);

    verify(client, never()).openTable("topic");

    verify(row, times(1)).addString("id", "a");
    verify(row, never()).addString("_table", TABLE_1);
    verify(session, times(1)).flush();
  }

  @Test
  public void testWriteShouldInsertKeyFields() throws KuduException {
    org.apache.kudu.Schema kuduSchema = new org.apache.kudu.Schema(Arrays.asList(
      new ColumnSchema.ColumnSchemaBuilder("ks", Type.STRING).build(),
      new ColumnSchema.ColumnSchemaBuilder("ki", Type.INT32).build(),
      new ColumnSchema.ColumnSchemaBuilder("int_field", Type.INT32).build()
    ));

    Schema keySchema = SchemaBuilder.struct().name("record")
      .version(1)
      .field("ks", STRING_SCHEMA)
      .field("ki", INT32_SCHEMA)
      .build();
    Struct key = new Struct(keySchema)
      .put("ks", "z")
      .put("ki", 9);

    Schema valueSchema = SchemaBuilder.struct().name("record")
      .version(1)
      .field("int_field", INT32_SCHEMA)
      .build();
    Struct value = new Struct(valueSchema)
      .put("int_field", 1);

    SinkRecord record = new SinkRecord("topic", 1, key.schema(), key, value.schema(), value, 1);

    when(client.newSession()).thenReturn(session);
    when(session.apply(upsert)).thenReturn(operationResponse);
    when(client.openTable("topic")).thenReturn(table);
    when(table.getSchema()).thenReturn(kuduSchema);
    when(table.newUpsert()).thenReturn(upsert);
    when(upsert.getRow()).thenReturn(row);

    Map<String,String> props = new HashMap<String, String>();
    props.put("kudu.master","0.0.0.0");
    props.put("kudu.key.insert", "true");
    KuduSinkConfig config = new KuduSinkConfig(props);

    KuduWriter writer = new KuduWriter(config, client);

    writer.write(Arrays.asList(record));

    verify(client, times(1)).openTable("topic");
    verify(row, times(1)).addString("ks", "z");
    verify(row, times(1)).addInt("ki", 9);
    verify(row, times(1)).addInt("int_field", 1);
    verify(session, times(1)).flush();
  }
}
