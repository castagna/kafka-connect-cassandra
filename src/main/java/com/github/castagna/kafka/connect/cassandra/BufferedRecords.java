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
import java.util.Collections;
import java.util.List;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.github.castagna.kafka.connect.cassandra.metadata.FieldsMetadata;
import com.github.castagna.kafka.connect.cassandra.metadata.SchemaPair;

public class BufferedRecords {
	private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);

	private final String keyspaceName;
	private final String tableName;
	private final CassandraSinkConnectorConfig config;
	private final Session session;

	private List<SinkRecord> records = new ArrayList<>();
	private SchemaPair currentSchemaPair;
	private FieldsMetadata fieldsMetadata;
	private PreparedStatement preparedStatement;
	private CassandraPreparedStatementBinder preparedStatementBinder;
	private CassandraStructure cassandraStructure;

	public BufferedRecords(CassandraSinkConnectorConfig config, String keyspaceName, String tableName, CassandraStructure cassandraStructure, Session session) {
		this.config = config;
		this.keyspaceName = keyspaceName;
		this.tableName = tableName;
		this.cassandraStructure = cassandraStructure;
		this.session = session;
	}

	public List<SinkRecord> add(SinkRecord record) {
		final SchemaPair schemaPair = new SchemaPair(record.keySchema(), record.valueSchema());

		if (currentSchemaPair == null) {
			currentSchemaPair = schemaPair;
			// re-initialize everything that depends on the record schema
			fieldsMetadata = FieldsMetadata.extract(tableName, config.pkMode, config.pkFields, config.fieldsWhitelist, config.fieldsBlacklist, currentSchemaPair);
			cassandraStructure.createOrAmendIfNecessary(config, session, keyspaceName, tableName, fieldsMetadata);
			final String statementCql = getCqlStatement(getOperation(config, record));
			log.trace("cql: {}", statementCql);
			preparedStatement = session.prepare(statementCql);
			preparedStatementBinder = new CassandraPreparedStatementBinder(config, schemaPair, fieldsMetadata);
		}

		final List<SinkRecord> flushed;
		if (currentSchemaPair.equals(schemaPair)) {
			// Continue with current batch state
			records.add(record);
			if (records.size() >= config.batchSize) {
				flushed = flush();
			} else {
				flushed = Collections.emptyList();
			}
		} else {
			// Each batch needs to have the same SchemaPair, so get the buffered records out, reset state and re-attempt the add
			flushed = flush();
			currentSchemaPair = null;
			flushed.addAll(add(record));
		}
		return flushed;
	}

	public static String getOperation(CassandraSinkConnectorConfig config, SinkRecord record) {
		return ((Struct)record.value()).getString(config.getString(CassandraSinkConnectorConfig.OPERATION_TYPE_FIELD_NAME));
	}
	
	public List<SinkRecord> flush() {
		if (records.isEmpty()) {
			return new ArrayList<>();
		}

		BatchStatement batch = new BatchStatement();
		for (SinkRecord record : records) {
			BoundStatement boundStatement = preparedStatement.bind();
			preparedStatementBinder.bindRecord(record, boundStatement);
			batch.add(boundStatement);
		}
		session.execute(batch);

		final List<SinkRecord> flushedRecords = records;
		records = new ArrayList<>();
		return flushedRecords;
	}

	public void close() {
		if (session != null) {
			session.close(); // TODO: should I do this??
			// TODO session = null;
		}
	}

	private String getCqlStatement(final String operation) {
		if ( operation.equals(config.getString(CassandraSinkConnectorConfig.OPERATION_TYPE_INSERT_VALUE)) ) {
			return cassandraStructure.getInsertStatement(keyspaceName, tableName, fieldsMetadata.keyFieldNames, fieldsMetadata.nonKeyFieldNames);
		} else if ( operation.equals(config.getString(CassandraSinkConnectorConfig.OPERATION_TYPE_UPDATE_VALUE)) ) {
			return cassandraStructure.getUpdateStatement(keyspaceName, tableName, fieldsMetadata.keyFieldNames, fieldsMetadata.nonKeyFieldNames);			
		} else if ( operation.equals(config.getString(CassandraSinkConnectorConfig.OPERATION_TYPE_DELETE_VALUE)) ) {
			return cassandraStructure.getDeleteStatement(keyspaceName, tableName, fieldsMetadata.keyFieldNames, fieldsMetadata.nonKeyFieldNames);
		} else {
			throw new ConnectException("Invalid operation type, please, check the ``" + CassandraSinkConnectorConfig.OPERATION_TYPE_FIELD_NAME + "`` and corresponding values for INSERT, UPDATE, and DELETE operations.");
		}
	}
}
