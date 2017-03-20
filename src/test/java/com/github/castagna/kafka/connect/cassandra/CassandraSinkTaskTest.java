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

import static dev.CassandraJavaAPIsTest.keyspaceCreate;
import static dev.CassandraJavaAPIsTest.keyspaceDelete;
import static dev.CassandraJavaAPIsTest.keyspaceExists;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class CassandraSinkTaskTest {

	private static CassandraSinkTask task;
	private static CassandraSinkConnectorConfig config;
	
	private static Cluster cluster = null;
	private static Session session = null;
	private static String keyspaceName = null;
	private static String tableName = null;	
	private final static long timestamp = new Date().getTime();
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		Properties properties = new Properties();
		properties.load(VersionUtil.class.getResourceAsStream("/cassandra-sink-quickstart.properties"));
		Map<String, String> settings = CassandraSinkConnectorConfigTest.properties2Map(properties);

		task = new CassandraSinkTask();
		task.start(settings);

		config = new CassandraSinkConnectorConfig(settings);

		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		session = cluster.newSession();

		keyspaceName = config.cassandraKeyspace;
		tableName = "table_test_" + timestamp;

    	if (!keyspaceExists(cluster, keyspaceName)) {
    		keyspaceCreate(session, keyspaceName);
    	}
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		task.stop();	
    	if (keyspaceExists(cluster, keyspaceName)) {
    		keyspaceDelete(session, keyspaceName);
    	}
		session.close();
		cluster.close();
	}

	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void testInsert() throws InterruptedException {
		// Write one record
		performInsert();
		
		// Read one record
    	Statement stmt = QueryBuilder.select().all().from(keyspaceName, tableName);
    	ResultSet rs = session.execute(stmt);
        rs = session.execute(stmt);
        int i = 0;
        while ( !rs.isExhausted() ) {
            Row row = rs.one();
            assertEquals(3, row.getColumnDefinitions().size());
            assertEquals("123456", row.getString("id"));
            assertEquals("Bobby McGee", row.getString("name"));
            assertEquals(21, row.getInt("age"));
            ++i;
        }
        assertEquals(1, i);
	}
	
	@Test
	public void testUpdate() throws InterruptedException {
		// Write one record
		performInsert();
		performUpdate();
		
		// Read one record
    	Statement stmt = QueryBuilder.select().all().from(keyspaceName, tableName);
    	ResultSet rs = session.execute(stmt);
        rs = session.execute(stmt);
        int i = 0;
        while ( !rs.isExhausted() ) {
            Row row = rs.one();
            assertEquals(3, row.getColumnDefinitions().size());
            assertEquals("123456", row.getString("id"));
            assertEquals("Bobby Mc Giver", row.getString("name"));
            assertEquals(51, row.getInt("age"));
            ++i;
        }
        assertEquals(1, i);
	}

	@Test
	public void testDelete() throws InterruptedException {
		// Write one record
		performInsert();
		performDelete();
		
		// Read one record
    	Statement stmt = QueryBuilder.select().all().from(keyspaceName, tableName);
    	ResultSet rs = session.execute(stmt);
        rs = session.execute(stmt);
        int i = 0;
        while ( !rs.isExhausted() ) {
            Row row = rs.one();
            assertEquals(3, row.getColumnDefinitions().size());
            assertEquals("123456", row.getString("id"));
            assertNull(row.getString("name"));
            assertEquals(21, row.getInt("age"));
            ++i;
        }
        assertEquals(1, i);
	}
	
	private static void performInsert() {
		// Write one record
		Collection<SinkRecord> records = new ArrayList<SinkRecord>();
		Schema keySchema = SchemaBuilder.struct().name(tableName + "_key")
				.field("id", Schema.STRING_SCHEMA)
				.build();
		Struct keyStruct = new Struct(keySchema)
				.put("id", "123456")
				;
		Schema valueSchema = SchemaBuilder.struct().name(tableName)
				// Oracle Golden Gate conventions
		        .field("table", Schema.STRING_SCHEMA)
		        .field("op_type", Schema.STRING_SCHEMA)
		        .field("op_ts", Schema.STRING_SCHEMA)
		        .field("current_ts", Schema.STRING_SCHEMA)
		        .field("pos", Schema.STRING_SCHEMA)
				.field("name", Schema.STRING_SCHEMA)
				.field("age", Schema.INT32_SCHEMA)
				.build();
		Struct valueStruct = new Struct(valueSchema)
				.put("table", tableName)
		        .put(config.getString(CassandraSinkConnectorConfig.OPERATION_TYPE_FIELD_NAME), config.getString(CassandraSinkConnectorConfig.OPERATION_TYPE_INSERT_VALUE))
		        .put("op_ts", "ts")
		        .put("current_ts", "current ts")
		        .put("pos", "0")
				.put("name", "Bobby McGee")
				.put("age", 21)
				;
		SinkRecord record = new SinkRecord("topic", 0, keySchema, keyStruct, valueSchema, valueStruct, 0);
		records.add(record);
		task.put(records);
	}
	
	private static void performUpdate() {
		// Update one record
		Collection<SinkRecord> records = new ArrayList<SinkRecord>();
		Schema keySchema = SchemaBuilder.struct().name(tableName + "_key")
				.field("id", Schema.STRING_SCHEMA)
				.build();
		Struct keyStruct = new Struct(keySchema)
				.put("id", "123456")
				;
		Schema valueSchema = SchemaBuilder.struct().name(tableName)
				// Oracle Golden Gate conventions
		        .field("table", Schema.STRING_SCHEMA)
		        .field("op_type", Schema.STRING_SCHEMA)
		        .field("op_ts", Schema.STRING_SCHEMA)
		        .field("current_ts", Schema.STRING_SCHEMA)
		        .field("pos", Schema.STRING_SCHEMA)
				.field("name", Schema.STRING_SCHEMA)
				.field("age", Schema.INT32_SCHEMA)
				.build();
		Struct valueStruct = new Struct(valueSchema)
				.put("table", tableName)
		        .put(config.getString(CassandraSinkConnectorConfig.OPERATION_TYPE_FIELD_NAME), config.getString(CassandraSinkConnectorConfig.OPERATION_TYPE_UPDATE_VALUE))
		        .put("op_ts", "ts")
		        .put("current_ts", "current ts")
		        .put("pos", "0")
				.put("name", "Bobby Mc Giver")
				.put("age", 51)
				;
		SinkRecord record = new SinkRecord("topic", 0, keySchema, keyStruct, valueSchema, valueStruct, 0);
		records.add(record);
		task.put(records);
	}

	private static void performDelete() {
		// Update one record
		Collection<SinkRecord> records = new ArrayList<SinkRecord>();
		Schema keySchema = SchemaBuilder.struct().name(tableName + "_key")
				.field("id", Schema.STRING_SCHEMA)
				.build();
		Struct keyStruct = new Struct(keySchema)
				.put("id", "123456")
				;
		Schema valueSchema = SchemaBuilder.struct().name(tableName)
				// Oracle Golden Gate conventions
		        .field("table", Schema.STRING_SCHEMA)
		        .field("op_type", Schema.STRING_SCHEMA)
		        .field("op_ts", Schema.STRING_SCHEMA)
		        .field("current_ts", Schema.STRING_SCHEMA)
		        .field("pos", Schema.STRING_SCHEMA)
				.field("name", Schema.STRING_SCHEMA)
				.build();
		Struct valueStruct = new Struct(valueSchema)
				.put("table", tableName)
		        .put(config.getString(CassandraSinkConnectorConfig.OPERATION_TYPE_FIELD_NAME), config.getString(CassandraSinkConnectorConfig.OPERATION_TYPE_DELETE_VALUE))
		        .put("op_ts", "ts")
		        .put("current_ts", "current ts")
		        .put("pos", "0")
				.put("name", "Bobby Mc Giver")
				;
		SinkRecord record = new SinkRecord("topic", 0, keySchema, keyStruct, valueSchema, valueStruct, 0);
		records.add(record);
		task.put(records);
	}
	
}
