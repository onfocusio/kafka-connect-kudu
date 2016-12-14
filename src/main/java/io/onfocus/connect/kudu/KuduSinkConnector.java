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

package io.onfocus.connect.kudu;

import io.onfocus.connect.kudu.sink.KuduSinkConfig;
import io.onfocus.connect.kudu.sink.KuduSinkTask;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KuduSinkConnector extends SinkConnector {
  private static final Logger log = LoggerFactory.getLogger(KuduSinkConnector.class);

  private Map<String, String> configProps;

  /**
   * Get the version of this connector.
   *
   * @return the version, formatted as a String
   */
  @Override
  public String version() {
    return this.getClass().getPackage().getImplementationVersion();
  }

  /**
   * Start this Connector. This method will only be called on a clean Connector, i.e. it has
   * either just been instantiated and initialized or {@link #stop()} has been invoked.
   *
   * @param props configuration settings
   */
  @Override
  public void start(Map<String, String> props) {
    log.info("\n" +
      "   ___        __                         \n" +
      "  /___\\_ __  / _| ___   ___ _   _ ___    \n" +
      " //  // '_ \\| |_ / _ \\ / __| | | / __|   \n" +
      "/ \\_//| | | |  _| (_) | (__| |_| \\__ \\   \n" +
      "\\___/ |_| |_|_|  \\___/ \\___|\\__,_|___/   \n" +
      "                 _       __ _       _    \n" +
      "  /\\ /\\_   _  __| |_   _/ _(_)_ __ | | __\n" +
      " / //_/ | | |/ _` | | | \\ \\| | '_ \\| |/ /\n" +
      "/ __ \\| |_| | (_| | |_| |\\ \\ | | | |   < \n" +
      "\\/  \\/ \\__,_|\\__,_|\\__,_\\__/_|_| |_|_|\\_\\\n" +
      "                                         \n" +
      "starting.");

    configProps = props;
  }

  /**
   * Returns the Task implementation for this Connector.
   */
  @Override
  public Class<? extends Task> taskClass() {
    return KuduSinkTask.class;
  }

  /**
   * Returns a set of configurations for Tasks based on the current configuration,
   * producing at most count configurations.
   *
   * @param maxTasks maximum number of configurations to generate
   * @return configurations for Tasks
   */
  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    log.info("Setting task configurations for {} workers.", maxTasks);
    final List<Map<String, String>> configs = new ArrayList<>(maxTasks);
    for (int i = 0; i < maxTasks; ++i) {
      configs.add(configProps);
    }
    return configs;
  }

  /**
   * Stop this connector.
   */
  @Override
  public void stop() {
  }

  /**
   * Define the configuration for the connector.
   *
   * @return The ConfigDef for this connector.
   */
  @Override
  public ConfigDef config() {
    return KuduSinkConfig.CONFIG_DEF;
  }
}
