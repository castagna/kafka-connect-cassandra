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

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.github.castagna.kafka.connect.cassandra.metadata.FieldsMetadata;
import com.github.castagna.kafka.connect.cassandra.metadata.SchemaPair;

public class BufferedRecords {
	private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);

	private final String tableName;
	private final CassandraSinkConnectorConfig config;
	private final Session session;

	private List<SinkRecord> records = new ArrayList<>();
	private SchemaPair currentSchemaPair;
	private FieldsMetadata fieldsMetadata;
	private PreparedStatement preparedStatement;
	private CassandraPreparedStatementBinder preparedStatementBinder;

	public BufferedRecords(CassandraSinkConnectorConfig config, String tableName, CassandraStructure cassandraStructure, Session session) {
		this.config = config;
		this.tableName = tableName;
		this.session = session;
	}

	public List<SinkRecord> add(SinkRecord record) {
		final SchemaPair schemaPair = new SchemaPair(record.keySchema(), record.valueSchema());

		if (currentSchemaPair == null) {
			currentSchemaPair = schemaPair;
			// re-initialize everything that depends on the record schema
			fieldsMetadata = FieldsMetadata.extract(tableName, config.pkMode, config.pkFields, config.fieldsWhitelist, currentSchemaPair);

			// TODO: dbStructure.createOrAmendIfNecessary(config, session,
			// tableName, fieldsMetadata);

			final String insertSql = getInsertSql();
			log.debug("{} sql: {}", config.insertMode, insertSql);
			close(); // TODO: why here???
			
//
//			RegularStatement toPrepare = new SimpleStatement("SELECT * FROM test WHERE k=?").setConsistencyLevel(ConsistencyLevel.QUORUM);
//			 PreparedStatement prepared = session.prepare(toPrepare);
//			 session.execute(prepared.bind("someValue"));
			 

			// TODO
			
//			preparedStatement = session.prepareStatement(insertSql);
//
//			preparedStatementBinder = new PreparedStatementBinder(preparedStatement, config.pkMode, schemaPair, fieldsMetadata);
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
			// Each batch needs to have the same SchemaPair, so get the buffered
			// records out, reset state and re-attempt the add
			flushed = flush();
			currentSchemaPair = null;
			flushed.addAll(add(record));
		}
		return flushed;
	}

	public List<SinkRecord> flush() {
		if (records.isEmpty()) {
			return new ArrayList<>();
		}
		for (SinkRecord record : records) {
			preparedStatementBinder.bindRecord(record);
		}

//		int totalUpdateCount = 0;
//		boolean successNoInfo = false;
//		for (int updateCount : preparedStatement.executeBatch()) {
//			if (updateCount == Statement.SUCCESS_NO_INFO) {
//				successNoInfo = true;
//				continue;
//			}
//			totalUpdateCount += updateCount;
//		}
//		if (totalUpdateCount != records.size() && !successNoInfo) {
//			switch (config.insertMode) {
//			case INSERT:
//				throw new ConnectException(String.format("Update count (%d) did not sum up to total number of records inserted (%d)", totalUpdateCount, records.size()));
//			case UPSERT:
//				log.trace("Upserted records:{} resulting in in totalUpdateCount:{}", records.size(), totalUpdateCount);
//			}
//		}
//		if (successNoInfo) {
//			log.info(
//					config.insertMode
//							+ " records:{} , but no count of the number of rows it affected is available",
//					records.size());
//		}

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

	private String getInsertSql() {
		switch (config.insertMode) {
		case INSERT:
			// TODO return dbDialect.getInsert(tableName, fieldsMetadata.keyFieldNames, fieldsMetadata.nonKeyFieldNames);
		case UPSERT:
			if (fieldsMetadata.keyFieldNames.isEmpty()) {
				throw new ConnectException(String.format("Write to table '%s' in UPSERT mode requires key field names to be known, check the primary key configuration", tableName));
			}
			// TODO return dbDialect.getUpsertQuery(tableName, fieldsMetadata.keyFieldNames, fieldsMetadata.nonKeyFieldNames);
		default:
			throw new ConnectException("Invalid insert mode");
		}
	}
}
