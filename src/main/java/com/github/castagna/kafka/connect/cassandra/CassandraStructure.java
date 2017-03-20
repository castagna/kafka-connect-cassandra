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
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;
import static com.github.castagna.kafka.connect.cassandra.StringBuilderUtil.Transform;
import static com.github.castagna.kafka.connect.cassandra.StringBuilderUtil.joinToBuilder;
import static com.github.castagna.kafka.connect.cassandra.StringBuilderUtil.nCopiesToBuilder;
import com.github.castagna.kafka.connect.cassandra.metadata.CassandraMetadataQueries;
import com.github.castagna.kafka.connect.cassandra.metadata.FieldsMetadata;
import com.github.castagna.kafka.connect.cassandra.metadata.SinkRecordField;
import com.github.castagna.kafka.connect.cassandra.metadata.Table;
import com.github.castagna.kafka.connect.cassandra.metadata.TableColumn;
import com.github.castagna.kafka.connect.cassandra.metadata.TableMetadataLoadingCache;

public class CassandraStructure {

	private final static Logger log = LoggerFactory.getLogger(CassandraStructure.class);

	private final TableMetadataLoadingCache tableMetadataLoadingCache = new TableMetadataLoadingCache();
	private final CqlDialect cqlDialect = new CqlDialect("\"", "\"");

	public CassandraStructure() {
	}

	public boolean createOrAmendIfNecessary(final CassandraSinkConnectorConfig config, final Session session, final String keyspaceName, final String tableName, final FieldsMetadata fieldsMetadata) {
		if (tableMetadataLoadingCache.get(session, keyspaceName, tableName) == null) {
			try {
				create(config, session, keyspaceName, tableName, fieldsMetadata);
			} catch (Exception e) {
				log.warn("Create failed, will attempt amend if table already exists", e);
				if (CassandraMetadataQueries.doesTableExist(session, keyspaceName, tableName)) {
					tableMetadataLoadingCache.refresh(session, keyspaceName, tableName);
				} else {
					throw e;
				}
			}
		}
		return amendIfNecessary(config, session, keyspaceName, tableName, fieldsMetadata, config.maxRetries);
	}

	void create(final CassandraSinkConnectorConfig config, final Session session, final String keyspaceName, final String tableName, final FieldsMetadata fieldsMetadata) {
		if (!config.autoCreate) {
			throw new ConnectException(String.format("Table %s is missing and auto-creation is disabled", tableName));
		}
		final String cql = cqlDialect.getCreateQuery(keyspaceName, tableName, fieldsMetadata.allFields.values());
		log.info("Creating table:{} with CQL: {}", tableName, cql);
		session.execute(cql);
		tableMetadataLoadingCache.refresh(session, keyspaceName, tableName);
	}

	boolean amendIfNecessary(final CassandraSinkConnectorConfig config, final Session session, final String keyspaceName, final String tableName, final FieldsMetadata fieldsMetadata, final int maxRetries) {
		// NOTE:
		// The table might have extra columns defined (hopefully with default values), which is not a case we check for here.
		// We also don't check if the data types for columns that do line-up are compatible.

		final Table tableMetadata = tableMetadataLoadingCache.get(session, keyspaceName, tableName);
		final Map<String, TableColumn> tableColumns = tableMetadata.columns;

		final Set<SinkRecordField> missingFields = missingFields(fieldsMetadata.allFields.values(), tableColumns.keySet());

		if (missingFields.isEmpty()) {
			return false;
		}

		for (SinkRecordField missingField : missingFields) {
			if (!missingField.isOptional() && missingField.defaultValue() == null) {
				throw new ConnectException(String.format("Cannot ALTER to add missing field (%s), as it is not optional and does not have a default value", missingField));
			}
		}

		if (!config.autoEvolve) {
			throw new ConnectException(String.format("Table %s is missing fields (%s) and auto-evolution is disabled", tableName, missingFields));
		}

		final List<String> amendTableQueries = new ArrayList<String>(); // TODO: dbDialect.getAlterTable(tableName, missingFields);
		log.info("Amending table to add missing fields:{} maxRetries:{} with CQL: {}", missingFields, maxRetries, amendTableQueries);
		try {
			for (String amendTableQuery : amendTableQueries) {
				session.execute(amendTableQuery);
			}
		} catch (Exception e) {
			if (maxRetries <= 0) {
				throw new ConnectException(String.format("Failed to amend table '%s' to add missing fields: %s", tableName, missingFields), e);
			}
			log.warn("Amend failed, re-attempting",e);
			tableMetadataLoadingCache.refresh(session, keyspaceName, tableName);
			// Perhaps there was a race with other tasks to add the columns
			return amendIfNecessary(config, session, keyspaceName, tableName, fieldsMetadata, maxRetries - 1);
		}

		tableMetadataLoadingCache.refresh(session, keyspaceName, tableName);
		return true;
	}

	Set<SinkRecordField> missingFields(Collection<SinkRecordField> fields, Set<String> dbColumnNames) {
		final Set<SinkRecordField> missingFields = new HashSet<>();
		for (SinkRecordField field : fields) {
			if (!dbColumnNames.contains(field.name())) {
				missingFields.add(field);
			}
		}
		return missingFields;
	}

	public final String getInsertStatement(String keyspaceName, String tableName, Set<String> keyFieldNames, Set<String> nonKeyFieldNames) {
	    StringBuilder builder = new StringBuilder("INSERT INTO ");
	    builder.append(keyspaceName + "." + tableName);
	    builder.append("(");
	    joinToBuilder(builder, ",", keyFieldNames, nonKeyFieldNames, escaper());
	    builder.append(") VALUES(");
	    nCopiesToBuilder(builder, ",", "?", keyFieldNames.size() + nonKeyFieldNames.size());
	    builder.append(")");
	    return builder.toString();
	}

	public final String getUpdateStatement(String keyspaceName, String tableName, Set<String> keyFieldNames, Set<String> nonKeyFieldNames) {
	    StringBuilder builder = new StringBuilder("UPDATE ");
	    builder.append(keyspaceName + "." + tableName);
	    
    	builder.append(" SET");
	    Iterator<String> iter = nonKeyFieldNames.iterator();
		while (iter.hasNext()) {
	    	builder.append(" ");
	    	builder.append(iter.next());
	    	builder.append("=?");
	    	if (iter.hasNext()) builder.append(",");
		}

	    builder.append(" WHERE ");
		iter = keyFieldNames.iterator();
		while (iter.hasNext()) {
	    	builder.append(iter.next());
	    	builder.append("=?");
	    	if (iter.hasNext()) builder.append(" AND ");
		}    
	    return builder.toString();
	}

	public final String getDeleteStatement(String keyspaceName, String tableName, Set<String> keyFieldNames, Set<String> nonKeyFieldNames) {
	    StringBuilder builder = new StringBuilder("DELETE ");

	    Iterator<String> iter = nonKeyFieldNames.iterator();
		while (iter.hasNext()) {
	    	builder.append(" ");
	    	builder.append(iter.next());
	    	if (iter.hasNext()) builder.append(",");
		}

		builder.append(" FROM ");
	    builder.append(keyspaceName + "." + tableName);

	    builder.append(" WHERE ");
	    iter = keyFieldNames.iterator();
		while (iter.hasNext()) {
	    	builder.append(iter.next());
	    	builder.append("=?");
	    	if (iter.hasNext()) builder.append(" AND ");
		}    
	    return builder.toString();
	}
	
	protected Transform<String> escaper() {
		return new Transform<String>() {
			@Override
			public void apply(StringBuilder builder, String identifier) {
				builder.append(cqlDialect.escapeStart).append(identifier).append(cqlDialect.escapeEnd);
			}
		};
	}

}
