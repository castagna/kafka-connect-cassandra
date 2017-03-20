package dev;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Before;
import org.junit.Test;

public class AvroMessageFormatterTest  {

    private final String schema = "{" +
    		"\"type\": \"record\", " +
    		"\"name\": \"my_schema\", " +
    		"\"fields\": [" +
    		"{\"name\": \"s\", \"type\": \"string\"}, " +
    		"{\"name\": \"b\",  \"type\": \"bytes\"}" +
    		"]}"; 

    private static final String s = "this is a string";
    private static final byte[] b = new String("this are bytes").getBytes();
    private static final Object o = b;
    
    private Schema.Parser parser;
    private Schema avroSchema;
    private ByteArrayOutputStream out;
    private Encoder encoder;
	private DatumWriter<GenericRecord> writer;
    
    @Before
    public void setup() {
    	parser =  new Schema.Parser();
    	avroSchema = parser.parse(schema);
		out = new ByteArrayOutputStream();
		encoder = EncoderFactory.get().binaryEncoder(out, null);
		writer = new GenericDatumWriter<>(avroSchema);
    }
    
	@Test
	public void testClassCastException_01() throws Exception {
		boolean thrown = false;
		try {
			GenericRecord datum = new GenericData.Record(avroSchema);
			datum.put("s", s);
			datum.put("b", b);
			writer.write(datum, encoder);
		} catch (ClassCastException e) {
			thrown = true;
		}
		assertTrue(thrown);
	}
	
	@Test
	public void testClassCastException_02() throws Exception {
		boolean thrown = false;
		try {
			GenericRecord datum = new GenericData.Record(avroSchema);
			datum.put("s", s);
			datum.put("b", o);
			writer.write(datum, encoder);
		} catch (ClassCastException e) {
			thrown = true;
		}
		assertTrue(thrown);
	}

	@Test
	public void testClassCastException_03() throws Exception {
		GenericRecord datum = new GenericData.Record(avroSchema);
		datum.put("s", s);
		datum.put("b", ByteBuffer.wrap(b));
		writer.write(datum, encoder);
	}
	
	@Test
	public void testClassCastException_04() throws Exception {
		GenericRecord datum = new GenericData.Record(avroSchema);
		datum.put("s", s);
		if ( o instanceof byte[] ) {
			datum.put("b", ByteBuffer.wrap(((byte[]) o)));
		} else {
			datum.put("b", o);			
		}
		writer.write(datum, encoder);
	}

}
