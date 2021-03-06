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

import com.datastax.driver.core.DataType;

public final class TableColumn {

	public final String name;
	public final boolean isPrimaryKey;
	public final boolean allowsNull;
	public final DataType cassandraDataType;

	public TableColumn(final String name, final boolean isPrimaryKey, final boolean allowsNull, final DataType cassandraDataType) {
		this.name = name;
		this.isPrimaryKey = isPrimaryKey;
		this.allowsNull = allowsNull;
		this.cassandraDataType = cassandraDataType;
	}

	@Override
	public String toString() {
		return "TableColumn{" + "name:'" + name + '\'' + ", " +
				"isPrimaryKey:" + isPrimaryKey + ", allowsNull:" + allowsNull + 
				", sqlType:" + cassandraDataType + '}';
	}

}