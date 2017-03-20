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

import static com.github.castagna.kafka.connect.cassandra.StringBuilderUtil.joinToBuilder;
import static com.github.castagna.kafka.connect.cassandra.StringBuilderUtil.nCopiesToBuilder;

import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.errors.ConnectException;

import com.github.castagna.kafka.connect.cassandra.StringBuilderUtil.Transform;
import com.github.castagna.kafka.connect.cassandra.metadata.SinkRecordField;

public class CqlDialect {

	public final String escapeStart;
	public final String escapeEnd;
	
	protected static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
	protected static final SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss.SSS");
	protected static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	CqlDialect(String escapeStart, String escapeEnd) {
		this.escapeStart = escapeStart;
		this.escapeEnd = escapeEnd;
	}

	public final String getInsert(final String tableName, final Collection<String> keyColumns, final Collection<String> nonKeyColumns) {
		StringBuilder builder = new StringBuilder("INSERT INTO ");
		builder.append(escaped(tableName));
		builder.append("(");
		joinToBuilder(builder, ",", keyColumns, nonKeyColumns, escaper());
		builder.append(") VALUES(");
		nCopiesToBuilder(builder, ",", "?", keyColumns.size() + nonKeyColumns.size());
		builder.append(")");
		return builder.toString();
	}
	
	public String getUpsertQuery(final String table, final Collection<String> keyColumns, final Collection<String> columns) {
		final StringBuilder builder = new StringBuilder();
		builder.append("merge into ");
		final String tableName = escaped(table);
		builder.append(tableName);
		builder.append(" using (select ");
		joinToBuilder(builder, ", ", keyColumns, columns, prefixedEscaper("? "));
		builder.append(" FROM dual) incoming on(");
		joinToBuilder(builder, " and ", keyColumns,
				new StringBuilderUtil.Transform<String>() {
					@Override
					public void apply(StringBuilder builder, String col) {
						builder.append(tableName).append(".")
								.append(escaped(col)).append("=incoming.")
								.append(escaped(col));
					}
				});
		builder.append(")");
		if (columns != null && columns.size() > 0) {
			builder.append(" when matched then update set ");
			joinToBuilder(builder, ",", columns,
					new StringBuilderUtil.Transform<String>() {
						@Override
						public void apply(StringBuilder builder, String col) {
							builder.append(tableName).append(".")
									.append(escaped(col)).append("=incoming.")
									.append(escaped(col));
						}
					});
		}

		builder.append(" when not matched then insert(");
		joinToBuilder(builder, ",", columns, keyColumns, prefixedEscaper(tableName + "."));
		builder.append(") values(");
		joinToBuilder(builder, ",", columns, keyColumns, prefixedEscaper("incoming."));
		builder.append(")");
		return builder.toString();
	}

	protected String escaped(String identifier) {
		return escapeStart + identifier + escapeEnd;
	}

	protected Transform<String> escaper() {
		return new Transform<String>() {
			@Override
			public void apply(StringBuilder builder, String identifier) {
				builder.append(escapeStart).append(identifier).append(escapeEnd);
			}
		};
	}
	
	protected Transform<String> prefixedEscaper(final String prefix) {
		return new Transform<String>() {
			@Override
			public void apply(StringBuilder builder, String identifier) {
				builder.append(prefix).append(escapeStart).append(identifier).append(escapeEnd);
			}
		};
	}

	protected void writeColumnSpec(StringBuilder builder, SinkRecordField f) {
		builder.append(escaped(f.name()));
		builder.append(" ");
		builder.append(getCassandraType(f.schemaName(), f.schemaParameters(), f.schemaType()));
//		if (f.defaultValue() != null) {
//			builder.append(" DEFAULT ");
//			formatColumnValue(builder, f.schemaName(), f.schemaParameters(), f.schemaType(), f.defaultValue());
//		} else if (f.isOptional()) {
//			builder.append(" NULL");
//		} else {
//			builder.append(" NOT NULL");
//		}
	}

	protected String getCassandraType(String schemaName, Map<String, String> parameters, Schema.Type type) {
		if (schemaName != null) {
			switch (schemaName) {
				case Date.LOGICAL_NAME:
					return "DATE";
				case Time.LOGICAL_NAME:
					return "DATE";
				case Timestamp.LOGICAL_NAME:
					return "TIMESTAMP";
			}
		}
		switch (type) {
			case INT8:
				return "TINYINT";
			case INT16:
				return "SMALLINT";
			case INT32:
				return "INT";
			case INT64:
				return "BIGINT";
			case FLOAT32:
				return "FLOAT";
			case FLOAT64:
				return "DOUBLE";
			case BOOLEAN:
				return "BOOLEAN";
			case STRING:
				return "TEXT";
			case BYTES:
				return "BLOB";
		}

		throw new ConnectException(String.format("%s (%s) type doesn't have a mapping to the SQL database column type", schemaName, type));
	}
	
	protected void formatColumnValue(StringBuilder builder, String schemaName, Map<String, String> schemaParameters, Schema.Type type, Object value) {
		if (schemaName != null) {
			switch (schemaName) {
			case Decimal.LOGICAL_NAME:
				builder.append(value);
				return;
			case Date.LOGICAL_NAME:
				builder.append("'")
						.append(DATE_FORMAT.format((java.util.Date) value))
						.append("'");
				return;
			case Time.LOGICAL_NAME:
				builder.append("'")
						.append(TIME_FORMAT.format((java.util.Date) value))
						.append("'");
				return;
			case Timestamp.LOGICAL_NAME:
				builder.append("'")
						.append(TIMESTAMP_FORMAT.format((java.util.Date) value))
						.append("'");
				return;
			}
		}
		switch (type) {
		case INT8:
		case INT16:
		case INT32:
		case INT64:
		case FLOAT32:
		case FLOAT64:
			// no escaping required
			builder.append(value);
			break;
		case BOOLEAN:
			// 1 & 0 for boolean is more portable rather than TRUE/FALSE
			builder.append((Boolean) value ? '1' : '0');
			break;
		case STRING:
			builder.append("'").append(value).append("'");
			break;
		case BYTES:
			final byte[] bytes;
			if (value instanceof ByteBuffer) {
				final ByteBuffer buffer = ((ByteBuffer) value).slice();
				bytes = new byte[buffer.remaining()];
				buffer.get(bytes);
			} else {
				bytes = (byte[]) value;
			}
			builder.append("x'")
					.append(DatatypeConverter.printHexBinary(bytes))
					.append("'");
			break;
		default:
			throw new ConnectException("Unsupported type for column value: " + type);
		}
	}

	static List<String> extractPrimaryKeyFieldNames(Collection<SinkRecordField> fields) {
		final List<String> pks = new ArrayList<>();
		for (SinkRecordField f : fields) {
			if (f.isPrimaryKey()) {
				pks.add(f.name());
			}
		}
		return pks;
	}	

	protected void writeColumnsSpec(StringBuilder builder, Collection<SinkRecordField> fields) {
		joinToBuilder(builder, ",", fields, new Transform<SinkRecordField>() {
			@Override
			public void apply(StringBuilder builder, SinkRecordField f) {
				builder.append(System.lineSeparator());
				writeColumnSpec(builder, f);
			}
		});
	}
	
	public String getCreateQuery(String keyspaceName, String tableName, Collection<SinkRecordField> fields) {
		final List<String> pkFieldNames = extractPrimaryKeyFieldNames(fields);
		final StringBuilder builder = new StringBuilder();
		builder.append("CREATE TABLE ");
		builder.append(keyspaceName + "." + tableName);
		builder.append(" (");
		writeColumnsSpec(builder, fields);
		if (!pkFieldNames.isEmpty()) {
			builder.append(",");
			builder.append(System.lineSeparator());
			builder.append("PRIMARY KEY(");
			joinToBuilder(builder, ",", pkFieldNames, escaper());
			builder.append(")");
		}
		builder.append(")");
		return builder.toString();
	}

	
}