package com.mfec.cpm.kafka.connect.smt;

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

public class SmtTemplateNoNewFieldTest {

    private SmtTemplateNoNewField<SourceRecord> xform = new SmtTemplateNoNewField.Value<>();

    @After
    public void tearDown() throws Exception {
        xform.close();
    }

    @Test(expected = DataException.class)
    public void topLevelStructRequired() {
        xform.configure(Collections.singletonMap("field.name", "field"));
        xform.apply(new SourceRecord(null, null, "", 0, Schema.INT32_SCHEMA, 42));
    }

    @Test
    public void testSchema() {
        final Map<String, Object> props = new HashMap<>();
        //TODO: ADD PROPERTIES
        props.put("field.name", "field");

//        xform.configure(props);

        //TODO: CREATE STRUCTURE AND VALUE
//        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("op", Schema.OPTIONAL_STRING_SCHEMA).build();
//        final Struct simpleStruct = new Struct(simpleStructSchema).put("op", "c");

//        final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
//        final SourceRecord transformedRecord = xform.apply(record);

    }

    @Test
    public void testSchemaless() {

    }

}