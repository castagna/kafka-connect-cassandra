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

import java.math.BigDecimal;
import java.nio.ByteBuffer;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.LocalDate;
import com.github.castagna.kafka.connect.cassandra.metadata.FieldsMetadata;
import com.github.castagna.kafka.connect.cassandra.metadata.SchemaPair;

public class CassandraPreparedStatementBinder {

	private final CassandraSinkConnectorConfig config;
	private final CassandraSinkConnectorConfig.PrimaryKeyMode pkMode;
	private final SchemaPair schemaPair;
	private final FieldsMetadata fieldsMetadata;
	
	private static final Logger log = LoggerFactory.getLogger(CassandraPreparedStatementBinder.class);

	public CassandraPreparedStatementBinder(CassandraSinkConnectorConfig config, SchemaPair schemaPair, FieldsMetadata fieldsMetadata) {
		this.config = config;
		this.pkMode = config.pkMode;
		this.schemaPair = schemaPair;
		this.fieldsMetadata = fieldsMetadata;
	}

	public void bindRecord(SinkRecord record, BoundStatement statement) {
		log.trace("Binding {} record...", record);
		
		final Struct valueStruct = (Struct) record.value();
		final String operation = BufferedRecords.getOperation(config, record);
		int index = 0;

		if ( operation.equals(config.getString(CassandraSinkConnectorConfig.OPERATION_TYPE_INSERT_VALUE)) ) {
			index = bindKeyFieldNames(statement, index++, record, valueStruct);
			index = bindNonKeyFieldNames(statement, index++, record, valueStruct);
		} else if ( operation.equals(config.getString(CassandraSinkConnectorConfig.OPERATION_TYPE_UPDATE_VALUE)) ) {
			index = bindNonKeyFieldNames(statement, index++, record, valueStruct);
			index = bindKeyFieldNames(statement, index++, record, valueStruct);
		} else if ( operation.equals(config.getString(CassandraSinkConnectorConfig.OPERATION_TYPE_DELETE_VALUE)) ) {
			index = bindKeyFieldNames(statement, index++, record, valueStruct);
		} else {
			throw new ConnectException("Invalid operation type, please, check the ``" + CassandraSinkConnectorConfig.OPERATION_TYPE_FIELD_NAME + "`` and corresponding values for INSERT, UPDATE, and DELETE operations.");
		}
	}
	
	private int bindKeyFieldNames(BoundStatement statement, int index, SinkRecord record, Struct valueStruct) {
		switch (pkMode) {
		case NONE:
			if (!fieldsMetadata.keyFieldNames.isEmpty()) {
				throw new AssertionError();
			}
			break;

		case KAFKA: {
			assert fieldsMetadata.keyFieldNames.size() == 3;
			bindField(statement, index++, Schema.STRING_SCHEMA, record.topic());
			bindField(statement, index++, Schema.INT32_SCHEMA, record.kafkaPartition());
			bindField(statement, index++, Schema.INT64_SCHEMA, record.kafkaOffset());
		}
			break;

		case RECORD_KEY: {
			if (schemaPair.keySchema.type().isPrimitive()) {
				assert fieldsMetadata.keyFieldNames.size() == 1;
				bindField(statement, index++, schemaPair.keySchema, record.key());
			} else {
				for (String fieldName : fieldsMetadata.keyFieldNames) {
					final Field field = schemaPair.keySchema.field(fieldName);
					bindField(statement, index++, field.schema(), ((Struct) record.key()).get(field));
				}
			}
		}
			break;

		case RECORD_VALUE: {
			for (String fieldName : fieldsMetadata.keyFieldNames) {
				final Field field = schemaPair.valueSchema.field(fieldName);
				bindField(statement, index++, field.schema(), ((Struct) record.value()).get(field));
			}
		}
			break;
		}
		
		return index;
	}
	
	private int bindNonKeyFieldNames(BoundStatement statement, int index, SinkRecord record, Struct valueStruct) {
		for (final String fieldName : fieldsMetadata.nonKeyFieldNames) {
			final Field field = record.valueSchema().field(fieldName);
			bindField(statement, index++, field.schema(), valueStruct.get(field));
		}
		return index;
	}

	void bindField(BoundStatement statement, int index, Schema schema, Object value) {
		if (value == null) {
			statement.set(index, null, Object.class);
		} else {
			final boolean bound = maybeBindLogical(statement, index, schema, value);
			if (!bound) {
				switch (schema.type()) {
				case INT8:
					statement.setByte(index, (Byte) value);
					break;
				case INT16:
					statement.setShort(index, (Short) value);
					break;
				case INT32:
					statement.setInt(index, (Integer) value);
					break;
				case INT64:
					statement.setLong(index, (Long) value);
					break;
				case FLOAT32:
					statement.setFloat(index, (Float) value);
					break;
				case FLOAT64:
					statement.setDouble(index, (Double) value);
					break;
				case BOOLEAN:
					statement.setBool(index, (Boolean) value);
					break;
				case STRING:
					statement.setString(index, (String) value);
					break;
				case BYTES:
					if (value instanceof ByteBuffer) {
						statement.setBytes(index, (ByteBuffer)value);
					} else {
						statement.setBytes(index, ByteBuffer.wrap((byte[])value));
					}
					break;
				default:
					throw new ConnectException("Unsupported source data type: " + schema.type());
				}
			}
		}
	}

	static boolean maybeBindLogical(BoundStatement statement, int index, Schema schema, Object value) {
		if (schema.name() != null) {
			switch (schema.name()) {
			case Date.LOGICAL_NAME:
				statement.setDate(index, LocalDate.fromMillisSinceEpoch(((java.util.Date) value).getTime()));
				return true;
			case Decimal.LOGICAL_NAME:
				statement.setDecimal(index, (BigDecimal) value);
				return true;
			case Time.LOGICAL_NAME:
				statement.setTime(index, ((java.util.Date) value).getTime());
				return true;
			case Timestamp.LOGICAL_NAME:
				statement.setTimestamp(index, new java.sql.Timestamp(((java.util.Date) value).getTime()));
				return true;
			default:
				return false;
			}
		}
		return false;
	}

}
