/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.castagna.kafka.connect.cassandra;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraSinkConnector extends SinkConnector {

	private static final Logger log = LoggerFactory.getLogger(CassandraSinkConnector.class);
	Map<String, String> config;

	@Override
	public String version() {
		return VersionUtil.getVersion();
	}

	@Override
	public void start(Map<String, String> properties) {
		this.config = properties;
		log.info("Starting Cassandra sink connector with properties:{}", properties);
	}

	@Override
	public Class<? extends Task> taskClass() {
		return CassandraSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		log.info("Setting task configurations for {} workers.", maxTasks);
		List<Map<String, String>> results = new ArrayList<Map<String, String>>();

		for (int i = 0; i < maxTasks; i++) {
			results.add(config);
		}

		return results;
	}

	@Override
	public void stop() {
		log.info("Stopping Cassandra sink connector.");
	}

	@Override
	public ConfigDef config() {
		return CassandraSinkConnectorConfig.config();
	}

}
