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
package com.github.castagna.kafka.connect.cassandra.metadata;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;

public class TableMetadataLoadingCache {

	private static final Logger log = LoggerFactory.getLogger(TableMetadataLoadingCache.class);

	private final Map<String, Table> cache = new HashMap<String, Table>();

	public Table get(final Session session, final String keyspaceName, final String tableName) {
		final String key = keyspaceName + "_" + tableName;
		Table table = cache.get(key);
		if (table == null) {
			if (CassandraMetadataQueries.doesTableExist(session, keyspaceName, tableName)) {
				table = CassandraMetadataQueries.getTableMetadata(session, keyspaceName, tableName);
				cache.put(key, table);
			} else {
				return null;
			}
		}
		return table;
	}

	public Table refresh(final Session session, final String keyspaceName, final String tableName) {
		final String key = keyspaceName + "_" + tableName;
		Table table = CassandraMetadataQueries.getTableMetadata(session, keyspaceName, tableName);
		log.info("Updating cached metadata for keyspace:{}, table:{}", keyspaceName, tableName);
		cache.put(key, table);
		return table;
	}

}
