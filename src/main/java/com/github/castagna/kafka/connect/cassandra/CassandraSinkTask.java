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

import java.util.Collection;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;

public class CassandraSinkTask extends SinkTask {

	private static final Logger log = LoggerFactory.getLogger(CassandraSinkTask.class);
	
	private CassandraSinkConnectorConfig config;
	
	private Cluster cluster;

	@Override
	public void start(Map<String, String> settings) {
	    log.info("Starting");
		this.config = new CassandraSinkConnectorConfig(settings);
	    log.info("Creating Cassandra client.");
	    this.cluster = Cluster.builder().addContactPoint(this.config.cassandraHost).build();
	    log.info("Created Cassandra client {}.", this.cluster);
	}
	
	@Override
	public void put(Collection<SinkRecord> collection) {

	    int count = 0;

	    for (SinkRecord record : collection) {
	    	Object key = record.key();
	    	Schema key_schema = record.keySchema();    	
	    	log.trace("key:{} with schema:{}", key, key_schema);
	    	Object value = record.value();
	    	Schema value_schema = record.valueSchema();
	    	log.trace("value:{} with schema:{}", value, value_schema);
	    	count++;
	    	
	    	// TODO: here we will need to get the info we need...
	    	
	    }
	    
	    log.trace("Received {} records", count);
	
	}

	@Override
	public void stop() {
		try {
			this.cluster.close();
		} catch (RuntimeException e) {
			log.error("Exception thrown while closing Cassandra client.", e);
		}
	}

	@Override
	public String version() {
		return VersionUtil.getVersion();
	}

}