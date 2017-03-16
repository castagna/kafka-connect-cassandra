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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;

public class CassandraSinkConnectorConfig extends AbstractConfig {

	public final InsertMode insertMode;
	public final PrimaryKeyMode pkMode;
	public final List<String> pkFields;
	public final Set<String> fieldsWhitelist;
	public final int batchSize;
	public final String tableNameFormat;
	public final boolean autoCreate;
	public final boolean autoEvolve;
	public final int maxRetries;
	
	public final String cassandraHost;
	public final String cassandraKeyspace;
	public final String cassandraUsername = null;
	public final String cassandraPassword = null;
	 
	
	public enum InsertMode {
		INSERT, UPSERT;
	}
	
	public enum PrimaryKeyMode {
		NONE, KAFKA, RECORD_KEY, RECORD_VALUE;
	}
	
	private static final String WRITES_GROUP = "Writes";
	private static final String DATAMAPPING_GROUP = "Data Mapping";
	private static final String DDL_GROUP = "DDL Support";
	private static final String RETRIES_GROUP = "Retries";
	
	public static final String CASSANDRA_KEYSPACE_NAME_CONFIG = "cassandra.keyspace.name";
	private static final String CASSANDRA_KEYSPACE_NAME_DOC = "Name of the Cassandra keyspace to write to.";
	public static final String CASSANDRA_HOST_CONFIG = "cassandra.host";
	private static final String CASSANDRA_HOST_DOC = "Host to connect to a Cassandra cluster.";	

	public static final List<String> DEFAULT_KAFKA_PK_NAMES = Arrays.asList("__connect_topic", "__connect_partition", "__connect_offset");

	public static final String TABLE_NAME_FORMAT = "table.name.format";
	private static final String TABLE_NAME_FORMAT_DEFAULT = "${topic}";
	private static final String TABLE_NAME_FORMAT_DOC =	"A format string for the destination table name, which may contain '${topic}' as a placeholder for the originating topic name.\n"
	      + "For example, ``kafka_${topic}`` for the topic 'orders' will map to the table name 'kafka_orders'.";
	private static final String TABLE_NAME_FORMAT_DISPLAY = "Table Name Format";
	
	public static final String BATCH_SIZE = "batch.size";
	private static final int BATCH_SIZE_DEFAULT = 3000;
	private static final String BATCH_SIZE_DOC = "Specifies how many records to attempt to batch together for insertion into the destination table, when possible.";
	private static final String BATCH_SIZE_DISPLAY = "Batch Size";
	
	public static final String PK_FIELDS = "pk.fields";
	private static final String PK_FIELDS_DEFAULT = "";
	private static final String PK_FIELDS_DOC = "List of comma-separated primary key field names. The runtime interpretation of this config depends on the ``pk.mode``:\n"
			+ "``none``\n"
			+ "    Ignored as no fields are used as primary key in this mode.\n"
			+ "``kafka``\n"
			+ "    Must be a trio representing the Kafka coordinates, defaults to ``" + StringUtils.join(DEFAULT_KAFKA_PK_NAMES, ",") + "`` if empty.\n"
			+ "``record_key``\n"
			+ "    If empty, all fields from the key struct will be used, otherwise used to extract the desired fields - for primitive key only a single field name must be configured.\n"
			+ "``record_value``\n"
			+ "    If empty, all fields from the value struct will be used, otherwise used to extract the desired fields.";
	private static final String PK_FIELDS_DISPLAY = "Primary Key Fields";
	
	public static final String PK_MODE = "pk.mode";
	private static final String PK_MODE_DEFAULT = "none";
	private static final String PK_MODE_DOC = "The primary key mode, also refer to ``" + PK_FIELDS + "`` documentation for interplay. Supported modes are:\n"
			+ "``none``\n"
			+ "    No keys utilized.\n"
			+ "``kafka``\n"
			+ "    Kafka coordinates are used as the PK.\n"
			+ "``record_key``\n"
			+ "    Field(s) from the record key are used, which may be a primitive or a struct.\n"
			+ "``record_value``\n"
			+ "    Field(s) from the record value are used, which must be a struct.";
	private static final String PK_MODE_DISPLAY = "Primary Key Mode";
	
	public static final String FIELDS_WHITELIST = "fields.whitelist";
	private static final String FIELDS_WHITELIST_DEFAULT = "";
	private static final String FIELDS_WHITELIST_DOC = "List of comma-separated record value field names. If empty, all fields from the record value are utilized, otherwise used to filter to the desired fields.\n"
	      + "Note that ``" + PK_FIELDS + "`` is applied independently in the context of which field(s) form the primary key columns in the destination database, while this configuration is applicable for the other columns.";
	private static final String FIELDS_WHITELIST_DISPLAY = "Fields Whitelist";	
	
	
	public static final String INSERT_MODE = "insert.mode";
	private static final String INSERT_MODE_DEFAULT = "insert";
	private static final String INSERT_MODE_DOC = "The insertion mode to use. Supported modes are:\n"
	      + "``insert``\n"
	      + "    Use standard SQL ``INSERT`` statements.\n"
	      + "``upsert``\n"
	      + "    Use the appropriate upsert semantics for the target database if it is supported by the connector, e.g. ``INSERT OR IGNORE``.";
	private static final String INSERT_MODE_DISPLAY = "Insert Mode";

	public static final String AUTO_CREATE = "auto.create";
	private static final String AUTO_CREATE_DEFAULT = "false";
	private static final String AUTO_CREATE_DOC = "Whether to automatically create the destination table based on record schema if it is found to be missing by issuing ``CREATE``.";
	private static final String AUTO_CREATE_DISPLAY = "Auto-Create";

	public static final String AUTO_EVOLVE = "auto.evolve";
	private static final String AUTO_EVOLVE_DEFAULT = "false";
	private static final String AUTO_EVOLVE_DOC = "Whether to automatically dd columns in the table schema when found to be missing relative to the record schema by issuing ``ALTER``.";
	private static final String AUTO_EVOLVE_DISPLAY = "Auto-Evolve";

	public static final String MAX_RETRIES = "max.retries";
	private static final int MAX_RETRIES_DEFAULT = 10;
	private static final String MAX_RETRIES_DOC = "The maximum number of times to retry on errors before failing the task.";
	private static final String MAX_RETRIES_DISPLAY = "Maximum Retries";

	protected CassandraSinkConnectorConfig(Map<String, String> properties) {
		super(config(), properties);

		cassandraHost = getString(CASSANDRA_HOST_CONFIG);
		cassandraKeyspace = getString(CASSANDRA_KEYSPACE_NAME_CONFIG);

		batchSize = getInt(BATCH_SIZE);
		insertMode = InsertMode.valueOf(getString(INSERT_MODE).toUpperCase());
	    pkMode = PrimaryKeyMode.valueOf(getString(PK_MODE).toUpperCase());
	    pkFields = getList(PK_FIELDS);
	    fieldsWhitelist = new HashSet<String>(getList(FIELDS_WHITELIST));
	    tableNameFormat = getString(TABLE_NAME_FORMAT).trim();
	    autoCreate = getBoolean(AUTO_CREATE);
	    autoEvolve = getBoolean(AUTO_EVOLVE);
	    maxRetries = getInt(MAX_RETRIES);
	}

	private static final ConfigDef.Range NON_NEGATIVE_INT_VALIDATOR = ConfigDef.Range.atLeast(0);
	
	public static ConfigDef config() {
		return new ConfigDef()
			.define(CASSANDRA_KEYSPACE_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, CASSANDRA_KEYSPACE_NAME_DOC)
			.define(CASSANDRA_HOST_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, CASSANDRA_HOST_DOC)
	        .define(INSERT_MODE, ConfigDef.Type.STRING, INSERT_MODE_DEFAULT, EnumValidator.in(InsertMode.values()), ConfigDef.Importance.HIGH, INSERT_MODE_DOC, WRITES_GROUP, 1, ConfigDef.Width.MEDIUM, INSERT_MODE_DISPLAY)
	        .define(PK_MODE, ConfigDef.Type.STRING, PK_MODE_DEFAULT, EnumValidator.in(PrimaryKeyMode.values()), ConfigDef.Importance.HIGH, PK_MODE_DOC, DATAMAPPING_GROUP, 2, ConfigDef.Width.MEDIUM, PK_MODE_DISPLAY)
	        .define(PK_FIELDS, ConfigDef.Type.LIST, PK_FIELDS_DEFAULT, ConfigDef.Importance.MEDIUM, PK_FIELDS_DOC, DATAMAPPING_GROUP, 3, ConfigDef.Width.LONG, PK_FIELDS_DISPLAY)
	        .define(FIELDS_WHITELIST, ConfigDef.Type.LIST, FIELDS_WHITELIST_DEFAULT, ConfigDef.Importance.MEDIUM, FIELDS_WHITELIST_DOC, DATAMAPPING_GROUP, 4, ConfigDef.Width.LONG, FIELDS_WHITELIST_DISPLAY)
	      	.define(BATCH_SIZE, ConfigDef.Type.INT, BATCH_SIZE_DEFAULT, NON_NEGATIVE_INT_VALIDATOR, ConfigDef.Importance.MEDIUM, BATCH_SIZE_DOC, WRITES_GROUP, 2, ConfigDef.Width.SHORT, BATCH_SIZE_DISPLAY)
			.define(TABLE_NAME_FORMAT, ConfigDef.Type.STRING, TABLE_NAME_FORMAT_DEFAULT, ConfigDef.Importance.MEDIUM, TABLE_NAME_FORMAT_DOC, DATAMAPPING_GROUP, 1, ConfigDef.Width.LONG, TABLE_NAME_FORMAT_DISPLAY)
			.define(AUTO_CREATE, ConfigDef.Type.BOOLEAN, AUTO_CREATE_DEFAULT, ConfigDef.Importance.MEDIUM, AUTO_CREATE_DOC, DDL_GROUP, 1, ConfigDef.Width.SHORT, AUTO_CREATE_DISPLAY)
			.define(AUTO_EVOLVE, ConfigDef.Type.BOOLEAN, AUTO_EVOLVE_DEFAULT, ConfigDef.Importance.MEDIUM, AUTO_EVOLVE_DOC, DDL_GROUP, 2, ConfigDef.Width.SHORT, AUTO_EVOLVE_DISPLAY)
			.define(MAX_RETRIES, ConfigDef.Type.INT, MAX_RETRIES_DEFAULT, NON_NEGATIVE_INT_VALIDATOR, ConfigDef.Importance.MEDIUM, MAX_RETRIES_DOC, RETRIES_GROUP, 1, ConfigDef.Width.SHORT, MAX_RETRIES_DISPLAY)
		;		
	}

	private static class EnumValidator implements ConfigDef.Validator {
		private final List<String> canonicalValues;
		private final Set<String> validValues;

		private EnumValidator(List<String> canonicalValues,
				Set<String> validValues) {
			this.canonicalValues = canonicalValues;
			this.validValues = validValues;
		}

		public static <E> EnumValidator in(E[] enumerators) {
			final List<String> canonicalValues = new ArrayList<String>(
					enumerators.length);
			final Set<String> validValues = new HashSet<>(
					enumerators.length * 2);
			for (E e : enumerators) {
				canonicalValues.add(e.toString().toLowerCase());
				validValues.add(e.toString().toUpperCase());
				validValues.add(e.toString().toLowerCase());
			}
			return new EnumValidator(canonicalValues, validValues);
		}

		@Override
		public void ensureValid(String key, Object value) {
			if (!validValues.contains(value)) {
				throw new ConfigException(key, value, "Invalid enumerator");
			}
		}

		@Override
		public String toString() {
			return canonicalValues.toString();
		}
	}
	
}
