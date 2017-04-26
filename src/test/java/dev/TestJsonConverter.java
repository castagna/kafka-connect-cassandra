package dev;

import static org.junit.Assert.assertNotNull;

import java.util.Collections;

import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestJsonConverter {

	private static final String TOPIC = "mytopic";
	
	@Before
	public void setup() {
		
	}
	
	@Test
	public void testJsonConverter() {
		JsonConverter converter = new JsonConverter();
		converter.configure(Collections.EMPTY_MAP, false);
		System.out.println(converter);
		SchemaAndValue sv = converter.toConnectData(TOPIC, "{ \"schema\": { \"type\": \"string\" }, \"payload\": \"...\" }".getBytes());
		assertNotNull(sv.schema());
	}
	
	@After
	public void teardown() {
		
	}
	
}
