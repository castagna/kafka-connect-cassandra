package dev;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Date;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

// This is for me... learning to use the Cassandra Java APIs...

public class TestCassandraJavaAPIs {
	
	private static Cluster cluster = null;
	private static Session session = null;
	private static String keyspaceName = null;
	private static String tableName = null;

	private static final Logger log = LoggerFactory.getLogger(TestCassandraJavaAPIs.class);
	
    @BeforeClass
    public static void setup() {
    	log.debug("setup() method called.");
	    cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
	    session = cluster.connect();
	    keyspaceName = "test_" + new Date().getTime();
	    tableName = "table_" + new Date().getTime();
    }
    
    @AfterClass
    public static void teardown() {
    	log.debug("teardown() method called.");
    	if (keyspaceExists(keyspaceName)) {
    		keyspaceDelete(keyspaceName);
    	}
    }

    @Test
    public void testTableExistanceCreationDeletion() {
    	assertFalse(keyspaceExists(keyspaceName));
    	assertFalse(tableExists(keyspaceName, tableName));

    	keyspaceCreate(keyspaceName);
    	assertTrue(keyspaceExists(keyspaceName));

    	assertFalse(tableExists(keyspaceName, tableName));
    	tableCreate(keyspaceName, tableName);
    	assertTrue(tableExists(keyspaceName, tableName));

    	tableDelete(keyspaceName, tableName);
    	assertFalse(tableExists(keyspaceName, tableName));

    	keyspaceDelete(keyspaceName);
    	assertFalse(keyspaceExists(keyspaceName));    
    }
    
    @Test
    public void testQueryBuilder() {
    	try {
        	keyspaceCreate(keyspaceName);
        	tableCreate(keyspaceName, tableName);

        	// Perform an insert
        	Insert insert = QueryBuilder.insertInto(keyspaceName, tableName);
        	insert.value("c1", 1);
        	insert.value("c2", "One");
        	session.execute(insert);
        	
        	// Perform a select
        	Statement stmt = QueryBuilder.select().all().from(keyspaceName, tableName);
            ResultSet rs = session.execute(stmt);
            
            int i = 0;
            while ( !rs.isExhausted() ) {
                Row row = rs.one();
                assertTrue(row.getInt(0) == 1);
                assertTrue(row.getString(1).equals("One"));
                ++i;
            }
            assertEquals(1, i);
            
        	// Perform an update
        	Statement stmtUpdate = QueryBuilder.update(keyspaceName, tableName)
        			.with(QueryBuilder.set("c2", "Uno"))
        			.where(QueryBuilder.eq("c1", 1));
        	session.execute(stmtUpdate);
        	
        	// Perform a select and check the update
        	stmt = QueryBuilder.select().all().from(keyspaceName, tableName);
            rs = session.execute(stmt);
            
            i = 0;
            while ( !rs.isExhausted() ) {
                Row row = rs.one();
                assertTrue(row.getInt(0) == 1);
                assertTrue(row.getString(1).equals("Uno"));
                ++i;
            }
            assertEquals(1, i);
            
            // Perform a delete

        	Statement stmtDelete = QueryBuilder.delete()
        			.from(keyspaceName, tableName)
        			.where(QueryBuilder.eq("c1", 1));
        	session.execute(stmtDelete);
        	
            i = 0;
            while ( !rs.isExhausted() ) {
                rs.one();
                ++i;
            }
            assertEquals(0, i);
    	} finally {
    		keyspaceDelete(keyspaceName);
    	}    	
    }
    
    @Test
    public void testQueryBuilderBatch() {
    	try {
        	keyspaceCreate(keyspaceName);
        	tableCreate(keyspaceName, tableName);    		
    		
        	Batch batch = QueryBuilder.batch();
        	
        	Insert stmtInsert = QueryBuilder.insertInto(keyspaceName, tableName);
        	stmtInsert.value("c1", 1);
        	stmtInsert.value("c2", "One");
        	batch.add(stmtInsert);

        	Update stmtUpdate = QueryBuilder.update(keyspaceName, tableName);
        	stmtUpdate.where(QueryBuilder.eq("c1", 1));
        	stmtUpdate.with(QueryBuilder.set("c2", "Uno"));
        	batch.add((Update)stmtUpdate);

        	session.execute(batch);

        	Statement stmt = QueryBuilder.select().all().from(keyspaceName, tableName);
        	ResultSet rs = session.execute(stmt);
            rs = session.execute(stmt);
            int i = 0;
            while ( !rs.isExhausted() ) {
                Row row = rs.one();
                assertTrue(row.getInt(0) == 1);
                assertTrue(row.getString(1).equals("Uno"));
                ++i;
            }
            assertEquals(1, i);
    	} finally {
    		keyspaceDelete(keyspaceName);
    	}      	
    }
    
    @Test
    public void testLogLevels() {
    	log.error("I am a ERROR level log message.");
    	log.warn("I am a WARN level log message.");
    	log.info("I am a INFO level log message.");
    	log.debug("I am a DEBUG level log message.");
    	log.trace("I am a TRACE level log message.");
    }

    private static boolean keyspaceExists (String keyspaceName) {
    	Metadata metadata = cluster.getMetadata();
    	List<KeyspaceMetadata> keyspacesMetadata = metadata.getKeyspaces();
    	for (KeyspaceMetadata keyspaceMetadata : keyspacesMetadata) {
			if ( keyspaceMetadata.getName().equals(keyspaceName) ) return true;
		}
    	return false;
    }

    private static boolean tableExists (String keyspaceName, String tableName) {
    	KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(keyspaceName);
    	if (keyspaceMetadata != null) {
        	TableMetadata tableMetadata = keyspaceMetadata.getTable(tableName);
        	return tableMetadata != null;    		
    	} else {
    		return false;
    	}
    }

    private static void keyspaceCreate (String keyspaceName) {
    	session.execute("CREATE KEYSPACE " + keyspaceName + " WITH replication = {'class':'SimpleStrategy','replication_factor':1}");
    }

    private static void keyspaceDelete (String keyspaceName) {
    	session.execute("DROP KEYSPACE " + keyspaceName);
    }

    private static void tableCreate (String keyspaceName, String tableName) {
    	session.execute("CREATE TABLE " + keyspaceName + "." + tableName + "( c1 int PRIMARY KEY, c2 text )");
    }

    private static void tableDelete (String keyspaceName, String tableName) {
    	session.execute("DROP TABLE " + keyspaceName + "." + tableName);
    }

}
