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

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class CassandraSinkTask extends SinkTask {

	private static final Logger log = LoggerFactory.getLogger(CassandraSinkTask.class);
	
	private CassandraSinkConnectorConfig config;
	private CassandraWriter writer;
	private Cluster cluster;
	private int remainingRetries;


	@Override
	public void start(Map<String, String> settings) {
	    log.info("Starting");
		this.config = new CassandraSinkConnectorConfig(settings);
	    remainingRetries = config.maxRetries;
	    initWriter();
	}
	
	void initWriter() {
	    log.info("Creating Cassandra client.");
	    this.cluster = Cluster.builder().addContactPoint(this.config.cassandraHost).build();
	    
	    // TODO: make the replication settings configurable!!
	    Session session = cluster.connect();
    	session.execute("CREATE KEYSPACE IF NOT EXISTS " + config.cassandraKeyspace + " WITH replication = {'class':'SimpleStrategy','replication_factor':1}");
	    
	    log.info("Created Cassandra client {}.", this.cluster);
	    this.writer = new CassandraWriter(config, new CassandraStructure());
	}
	
	@Override
	public void put(Collection<SinkRecord> records) {
		if (records.isEmpty()) {
			return;
		}
		final SinkRecord first = records.iterator().next();
		final int recordsCount = records.size();
		log.trace("Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to Cassandra...", recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset());
		try {
			writer.write(records);
		} catch (Exception e) {
			log.warn("Write of {} records failed, remainingRetries={}", records.size(), remainingRetries, e);
			if (remainingRetries == 0) {
				throw new ConnectException(e);
			} else {
				writer.closeQuietly();
				initWriter();
				remainingRetries--;
				// context.timeout(10000); // TODO: make this a configurable option
				throw new RetriableException(e);
			}
		}
		remainingRetries = config.maxRetries;
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