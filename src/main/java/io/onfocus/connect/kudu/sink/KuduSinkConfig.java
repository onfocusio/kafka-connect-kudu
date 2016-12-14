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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class KuduSinkConfig extends AbstractConfig {

  public static final String KUDU_MASTER = "kudu.master";
  private static final String KUDU_MASTER_DOC = "Comma-separated list of \"host:port\" pairs of the masters.";

  public static final String KUDU_WORKER_COUNT = "kudu.worker.count";
  private static final String KUDU_WORKER_COUNT_DOC = "Maximum number of worker threads. Defauts to \"2 * the number of available processors\".";
  private static final int KUDU_WORKER_COUNT_DEFAULT = -1;

  public static final String KUDU_TABLE_FIELD = "kudu.table.field";
  private static final String KUDU_TABLE_FIELD_DOC = "Record field defining the target table name. Defaults to the topic name of the current record.";

  private static final String CONNECTION_GROUP = "Connection";
  private static final String DATA_MAPPING_GROUP = "Data Mapping";

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
    // Connection
    .define(
      KUDU_MASTER, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE,
      ConfigDef.Importance.HIGH, KUDU_MASTER_DOC,
      CONNECTION_GROUP, 1, ConfigDef.Width.MEDIUM, KUDU_MASTER)
    .define(
      KUDU_WORKER_COUNT, ConfigDef.Type.INT, KUDU_WORKER_COUNT_DEFAULT,
      ConfigDef.Importance.MEDIUM, KUDU_WORKER_COUNT_DOC,
      CONNECTION_GROUP, 2, ConfigDef.Width.MEDIUM, KUDU_WORKER_COUNT)
    // Data Mapping
    .define(
      KUDU_TABLE_FIELD, ConfigDef.Type.STRING, null,
      ConfigDef.Importance.LOW, KUDU_TABLE_FIELD_DOC,
      DATA_MAPPING_GROUP, 1, ConfigDef.Width.MEDIUM, KUDU_TABLE_FIELD);

  public final String kuduMaster;
  public final Integer kuduWorkerCount;
  public final String kuduTableField;

  public KuduSinkConfig(Map<?, ?> props) {
    super(CONFIG_DEF, props);
    kuduMaster = getString(KUDU_MASTER);
    kuduWorkerCount = getInt(KUDU_WORKER_COUNT);
    kuduTableField = getString(KUDU_TABLE_FIELD);
  }

  public static void main(String... args) {
    System.out.println(CONFIG_DEF.toRst());
  }
}
