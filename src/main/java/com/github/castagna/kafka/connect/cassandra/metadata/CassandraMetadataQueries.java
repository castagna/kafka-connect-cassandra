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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

public abstract class CassandraMetadataQueries {

	private static final Logger log = LoggerFactory.getLogger(CassandraMetadataQueries.class);

	public static boolean doesTableExist (final Session session, final String keyspaceName, final String tableName) {
		final Metadata metadata = session.getCluster().getMetadata();
		final KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(keyspaceName);

		log.info("Checking table:{} exists in the keyspace:{}", tableName, keyspaceName);

		boolean exists = false;

		if (keyspaceMetadata != null) {
			final TableMetadata tableMetadata = keyspaceMetadata.getTable(tableName);
			exists = tableMetadata != null;
			log.info("table:{} is {} in keyspace:{}", tableName, exists ? "present" : "absent", keyspaceName);
		} else {
			log.info("table:{} is absent in keyspace:{}, keyspace does not exist", tableName, keyspaceName);
		}

		return exists;
	}

	public static Table getTableMetadata(final Session session, final String keyspaceName, final String tableName) {
	    final List<TableColumn> columns = new ArrayList<TableColumn>();

		final Metadata metadata = session.getCluster().getMetadata();
		final KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(keyspaceName);

		if (keyspaceMetadata != null) {
			final TableMetadata tableMetadata = keyspaceMetadata.getTable(tableName);
			if (tableMetadata != null) {
				final Set<String> pkColumns = new HashSet<String>();
			    List<ColumnMetadata> columnsPrimaryKeyMetadata = tableMetadata.getPrimaryKey();
			    for (ColumnMetadata columnPrimaryKeyMetadata : columnsPrimaryKeyMetadata) {
					pkColumns.add(columnPrimaryKeyMetadata.getName());
				}

				List<ColumnMetadata> columnsMetadata = tableMetadata.getColumns();
				for (ColumnMetadata columnMetadata : columnsMetadata) {
			        final String columnName = columnMetadata.getName();
			        final DataType columnType = columnMetadata.getType();
			        final boolean isPk = pkColumns.contains(columnName);
			        final boolean isNullable = !isPk;
			        columns.add(new TableColumn(columnName, isPk, isNullable, columnType));
				}				
			}
		}

		return new Table(tableName, columns);
	}
	
}