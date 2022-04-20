package org.wave7.kafka;

import junit.framework.TestCase;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MaskOperationTest {
    private MaskOperation<SourceRecord> xform = new MaskOperation.Value<>();

    @After
    public void tearDown() throws Exception {
        xform.close();
    }

    @Test(expected = DataException.class)
    public void topLevelStructRequired() {
        xform.configure(Collections.singletonMap("operation.field.name", "op"));
        xform.apply(new SourceRecord(null, null, "", 0, Schema.INT32_SCHEMA, 42));
    }

    @Test
    public void copySchemaAndInsertUuidField() {
        final Map<String, Object> props = new HashMap<>();
        props.put("operation.field.name", "op");

        xform.configure(props);
//
        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("op", Schema.OPTIONAL_STRING_SCHEMA).build();
        final Struct simpleStruct = new Struct(simpleStructSchema).put("op", "c");
//
        final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
        final SourceRecord transformedRecord = xform.apply(record);
//
//        assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
//        assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
//        assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());
//
//        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformedRecord.valueSchema().field("op").schema());
//        assertEquals("initial", ((Struct) transformedRecord.value()).get("ReqType"));
//        assertEquals(Schema.STRING_SCHEMA, transformedRecord.valueSchema().field("ReqType").schema());
//        assertNotNull(((Struct) transformedRecord.value()).getString("ReqType"));
//
//        // Exercise caching
//        final SourceRecord transformedRecord2 = xform.apply(
//                new SourceRecord(null, null, "test", 1, simpleStructSchema, new Struct(simpleStructSchema)));
//        assertSame(transformedRecord.valueSchema(), transformedRecord2.valueSchema());

    }

    @Test
    public void schemalessInsertUuidField() {
//        final Map<String, Object> props = new HashMap<>();
//
//        props.put("operation.field.name", "op");
//
//        xform.configure(props);
//
//        final SourceRecord record = new SourceRecord(null, null, "test", 0,
//                null, Collections.singletonMap("op", "c"));
//
//        final SourceRecord transformedRecord = xform.apply(record);
//        assertEquals("initial", ((Map) transformedRecord.value()).get("ReqType"));
//        assertNotNull(((Map) transformedRecord.value()).get("ReqType"));

    }

}