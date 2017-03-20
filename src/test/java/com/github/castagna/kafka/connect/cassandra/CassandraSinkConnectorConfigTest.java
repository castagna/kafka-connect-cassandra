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

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

public class CassandraSinkConnectorConfigTest {

	@Test
	public void testCassandraSinkConnectorConfig() throws IOException {
	      Properties properties = new Properties();
	      properties.load(VersionUtil.class.getResourceAsStream("/cassandra-sink-quickstart.properties"));
	      
	      CassandraSinkConnectorConfig config = new CassandraSinkConnectorConfig(properties2Map(properties));
	      
	      properties.containsKey(CassandraSinkConnectorConfig.CASSANDRA_HOST_CONFIG);
	      properties.containsKey(CassandraSinkConnectorConfig.CASSANDRA_KEYSPACE_NAME_CONFIG);
	      assertEquals("com.github.castagna.kafka.connect.cassandra.CassandraSinkConnector", properties.getProperty("connector.class"));

	      assertEquals("[table, op_type, op_ts, current_ts, pos]", config.getList(CassandraSinkConnectorConfig.FIELDS_BLACKLIST).toString());
	      assertEquals("test_cassandra_sink", config.getString(CassandraSinkConnectorConfig.CASSANDRA_KEYSPACE_NAME_CONFIG));

	      assertEquals("op_type", config.getString(CassandraSinkConnectorConfig.OPERATION_TYPE_FIELD_NAME));
	      assertEquals("I", config.getString(CassandraSinkConnectorConfig.OPERATION_TYPE_INSERT_VALUE));
	      assertEquals("U", config.getString(CassandraSinkConnectorConfig.OPERATION_TYPE_UPDATE_VALUE));
	      assertEquals("D", config.getString(CassandraSinkConnectorConfig.OPERATION_TYPE_DELETE_VALUE));
	}

	public static Map<String,String> properties2Map (Properties properties) {
	      Map<String,String> map = new HashMap<String,String>();
	      for (Map.Entry<Object, Object> e : properties.entrySet()) {
	    	  String key = (String) e.getKey();
	    	  String value = (String) e.getValue();
	    	  map.put(key, value);
	      }
	      return map;
	}
	
}
