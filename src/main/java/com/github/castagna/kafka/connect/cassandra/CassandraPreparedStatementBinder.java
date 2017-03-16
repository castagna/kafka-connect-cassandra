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

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedStatement;
import com.github.castagna.kafka.connect.cassandra.metadata.FieldsMetadata;
import com.github.castagna.kafka.connect.cassandra.metadata.SchemaPair;

public class CassandraPreparedStatementBinder {

  private final CassandraSinkConnectorConfig.PrimaryKeyMode pkMode;
  private final PreparedStatement preparedStatement;
  private final SchemaPair schemaPair;
  private final FieldsMetadata fieldsMetadata;

	public CassandraPreparedStatementBinder(PreparedStatement statement, CassandraSinkConnectorConfig.PrimaryKeyMode pkMode, SchemaPair schemaPair, FieldsMetadata fieldsMetadata) {
		this.pkMode = pkMode;
		this.preparedStatement = statement;
		this.schemaPair = schemaPair;
		this.fieldsMetadata = fieldsMetadata;
	}

	public void bindRecord(SinkRecord record) {
		final Struct valueStruct = (Struct) record.value();

		// Assumption: the relevant SQL has placeholders for keyFieldNames first
		// followed by nonKeyFieldNames, in iteration order

		int index = 1;

		switch (pkMode) {
		case NONE:
			if (!fieldsMetadata.keyFieldNames.isEmpty()) {
				throw new AssertionError();
			}
			break;

		case KAFKA: {
			assert fieldsMetadata.keyFieldNames.size() == 3;
			bindField(index++, Schema.STRING_SCHEMA, record.topic());
			bindField(index++, Schema.INT32_SCHEMA, record.kafkaPartition());
			bindField(index++, Schema.INT64_SCHEMA, record.kafkaOffset());
		}
			break;

		case RECORD_KEY: {
			if (schemaPair.keySchema.type().isPrimitive()) {
				assert fieldsMetadata.keyFieldNames.size() == 1;
				bindField(index++, schemaPair.keySchema, record.key());
			} else {
				for (String fieldName : fieldsMetadata.keyFieldNames) {
					final Field field = schemaPair.keySchema.field(fieldName);
					bindField(index++, field.schema(),
							((Struct) record.key()).get(field));
				}
			}
		}
			break;

		case RECORD_VALUE: {
			for (String fieldName : fieldsMetadata.keyFieldNames) {
				final Field field = schemaPair.valueSchema.field(fieldName);
				bindField(index++, field.schema(),
						((Struct) record.value()).get(field));
			}
		}
			break;
		}

		for (final String fieldName : fieldsMetadata.nonKeyFieldNames) {
			final Field field = record.valueSchema().field(fieldName);
			bindField(index++, field.schema(), valueStruct.get(field));
		}

		// preparedStatement.addBatch(); // TODO: is this needed with Cassandra?
	}

	void bindField(int index, Schema schema, Object value) {
		bindField(preparedStatement, index, schema, value);
	}

	static void bindField(PreparedStatement preparedStatement, int index, Schema schema, Object value) {
		BoundStatement statement = preparedStatement.bind();
		if (value == null) {
			// statement.setObject(index, null); // TODO: what do I do here???
		} else {
			final boolean bound = maybeBindLogical(preparedStatement, index,
					schema, value);
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
					final ByteBuffer bytes;
					if (value instanceof ByteBuffer) {
						bytes = (ByteBuffer) value;
					} else {
						bytes = ByteBuffer.wrap((byte[]) value);
					}
					statement.setBytes(index, bytes);
					break;
				default:
					throw new ConnectException("Unsupported source data type: "
							+ schema.type());
				}
			}
		}
	}

	static boolean maybeBindLogical(PreparedStatement preparedStatement, int index, Schema schema, Object value) {
		BoundStatement statement = preparedStatement.bind();
		if (schema.name() != null) {
			switch (schema.name()) {
			case Date.LOGICAL_NAME:
				statement.setDate(index, LocalDate
						.fromMillisSinceEpoch(((java.util.Date) value)
								.getTime()));
				return true;
			case Decimal.LOGICAL_NAME:
				statement.setDecimal(index, (BigDecimal) value);
				return true;
			case Time.LOGICAL_NAME:
				statement.setTime(index, ((java.util.Date) value).getTime());
				return true;
			case Timestamp.LOGICAL_NAME:
				statement.setTimestamp(index, (java.util.Date) value);
				return true;
			default:
				return false;
			}
		}
		return false;
	}

}
