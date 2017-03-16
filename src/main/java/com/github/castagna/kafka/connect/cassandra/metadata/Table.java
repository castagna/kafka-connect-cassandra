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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Table {

	public final String name;
	public final Map<String, TableColumn> columns = new HashMap<String, TableColumn>();
	public final Set<String> primaryKeyColumnNames = new HashSet<String>();

	public Table(String name, List<TableColumn> columns) {
		this.name = name;
		for (final TableColumn column : columns) {
			this.columns.put(column.name, column);
			if (column.isPrimaryKey) {
				primaryKeyColumnNames.add(column.name);
			}
		}
	}

	@Override
	public String toString() {
		return "Table{" + "name:'" + name + '\'' + ", columns:" + columns + '}';
	}

}