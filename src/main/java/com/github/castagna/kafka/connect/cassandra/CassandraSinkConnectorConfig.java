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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraSinkConnectorConfig extends AbstractConfig {

	private static final Logger log = LoggerFactory.getLogger(CassandraSinkConnectorConfig.class);
	
	public enum PrimaryKeyMode {
		NONE, KAFKA, RECORD_KEY, RECORD_VALUE;
	}
	
	public static final String TABLE_NAME_CONFIG = "cassandra.table.name";
	private static final String TABLE_NAME_DOC = "Name of the Cassandra table to write to.";
	public static final String CASSANDRA_HOST_CONFIG = "cassandra.host";
	private static final String CASSANDRA_HOST_DOC = "Host to connect to a Cassandra cluster.";
	

	public static final List<String> DEFAULT_KAFKA_PK_NAMES = Arrays.asList(
			"__connect_topic", "__connect_partition", "__connect_offset");

	public static final String PK_FIELDS = "pk.fields";
	private static final String PK_FIELDS_DEFAULT = "";
	private static final String PK_FIELDS_DOC = "List of comma-separated primary key field names. The runtime interpretation of this config depends on the ``pk.mode``:\n"
			+ "``none``\n"
			+ "    Ignored as no fields are used as primary key in this mode.\n"
			+ "``kafka``\n"
			+ "    Must be a trio representing the Kafka coordinates, defaults to ``"
			+ StringUtils.join(DEFAULT_KAFKA_PK_NAMES, ",")
			+ "`` if empty.\n"
			+ "``record_key``\n"
			+ "    If empty, all fields from the key struct will be used, otherwise used to extract the desired fields - for primitive key only a single field name must be configured.\n"
			+ "``record_value``\n"
			+ "    If empty, all fields from the value struct will be used, otherwise used to extract the desired fields.";
	private static final String PK_FIELDS_DISPLAY = "Primary Key Fields";
	
	public static final String PK_MODE = "pk.mode";
	private static final String PK_MODE_DEFAULT = "none";
	private static final String PK_MODE_DOC = "The primary key mode, also refer to ``"
			+ PK_FIELDS
			+ "`` documentation for interplay. Supported modes are:\n"
			+ "``none``\n"
			+ "    No keys utilized.\n"
			+ "``kafka``\n"
			+ "    Kafka coordinates are used as the PK.\n"
			+ "``record_key``\n"
			+ "    Field(s) from the record key are used, which may be a primitive or a struct.\n"
			+ "``record_value``\n"
			+ "    Field(s) from the record value are used, which must be a struct.";
	private static final String PK_MODE_DISPLAY = "Primary Key Mode";
	
	protected CassandraSinkConnectorConfig(ConfigDef configDef, Map<String, String> properties) {
		super(configDef, properties);
	}

	public static ConfigDef config() {
		return new ConfigDef()
			.define(TABLE_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, TABLE_NAME_DOC)
			.define(CASSANDRA_HOST_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, CASSANDRA_HOST_DOC);
	}

}
