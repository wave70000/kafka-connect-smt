package org.wave7.kafka;

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

import static org.junit.Assert.*;

public class convertOperationTest {

    private ConvertOperation<SourceRecord> xform = new ConvertOperation.Value<>();

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

        final Schema sourceSchema = SchemaBuilder.struct().name("source").version(1).doc("doc")
                .field("name",Schema.OPTIONAL_STRING_SCHEMA)
                .field("db",Schema.OPTIONAL_STRING_SCHEMA)
                .field("table",Schema.OPTIONAL_STRING_SCHEMA);
        final Struct sourceValue = new Struct(sourceSchema)
                .put("name","fullfillment")
                .put("db","TutorialDB")
                .put("table","Employees");

        final Schema valueSchema = SchemaBuilder.struct().name("name").version(1).doc("doc")
                .field("param",Schema.OPTIONAL_STRING_SCHEMA)
                .field("source",sourceSchema)
                .field("op",Schema.OPTIONAL_STRING_SCHEMA)
                .field("ts_ms",Schema.OPTIONAL_STRING_SCHEMA)
                .field("requestAppId",Schema.OPTIONAL_STRING_SCHEMA);
        final Struct value = new Struct(valueSchema)
                .put("param","testParam")
                .put("source",sourceValue)
                .put("op","c")
                .put("ts_ms", "1650444582336")
                .put("requestAppId","HRMI");


        final SourceRecord mssql = new SourceRecord(null,null,"test",0,valueSchema,value);
        final SourceRecord transformRecordMssql = xform.apply(mssql);
//        System.out.println(transformRecordMssql);
//        System.out.println("\n"+mssql+"\n");
//        final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
//        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals(simpleStructSchema.name(), transformRecordMssql.valueSchema().name());
        assertEquals(simpleStructSchema.version(), transformRecordMssql.valueSchema().version());
        assertEquals(simpleStructSchema.doc(), transformRecordMssql.valueSchema().doc());

        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformRecordMssql.valueSchema().field("op").schema());
        assertEquals("Add", ((Struct) transformRecordMssql.value()).get("ReqType"));
//        assertEquals(Schema.STRING_SCHEMA, transformRecordMssql.valueSchema().field("ReqType").schema());
        assertNotNull(((Struct) transformRecordMssql.value()).getString("ReqType"));

//        ((Struct) transformRecordMssql.value).getStruct("source").get("table");

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