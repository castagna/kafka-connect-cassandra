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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;

import com.datastax.driver.core.Session;

public class CassandraWriter {

	private final CassandraSinkConnectorConfig config;
	private final CassandraStructure cassandraStructure;
	final CachedSessionProvider cachedSessionProvider;

	CassandraWriter(final CassandraSinkConnectorConfig config, CassandraStructure cassandraStructure) {
		this.config = config;
		this.cassandraStructure = cassandraStructure;

		this.cachedSessionProvider = new CachedSessionProvider(config.cassandraHost, config.cassandraUsername, config.cassandraPassword);
	}

	void write(final Collection<SinkRecord> records) {
		final Session session = cachedSessionProvider.getValidConnection();

		final Map<String, BufferedRecords> bufferByTable = new HashMap<>();
		for (SinkRecord record : records) {
			final String table = destinationTable(record.topic());
			BufferedRecords buffer = bufferByTable.get(table);
			if (buffer == null) {
				buffer = new BufferedRecords(config, table, cassandraStructure, session);
				bufferByTable.put(table, buffer);
			}
			buffer.add(record);
		}
		for (BufferedRecords buffer : bufferByTable.values()) {
			buffer.flush();
			buffer.close();
		}
	}

	void closeQuietly() {
		cachedSessionProvider.closeQuietly();
	}

	String destinationTable(String topic) {
		final String tableName = config.tableNameFormat.replace("${topic}", topic);
		if (tableName.isEmpty()) {
			throw new ConnectException(
					String.format("Destination table name for topic '%s' is empty using the format string '%s'",
							topic, 
							config.tableNameFormat));
		}
		return tableName;
	}

}
