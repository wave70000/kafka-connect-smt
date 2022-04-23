package com.mfec.cpm.kafka.connect.smt;

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

import static org.junit.Assert.assertEquals;

public class CopyFieldTest {

    private CopyField<SourceRecord> xform = new CopyField.Value<>();

    @After
    public void tearDown() throws Exception {
        xform.close();
    }

    @Test(expected = DataException.class)
    public void topLevelStructRequired() {
        xform.configure(Collections.singletonMap("copy.field", "before"));
        xform.configure(Collections.singletonMap("new.field", "after"));
        xform.apply(new SourceRecord(null, null, "", 0, Schema.INT32_SCHEMA, 42));
    }

    @Test
    public void copySchemaAndInsertUuidField() {
        final String oldField = "before";
        final String newField = "after";
        final Map<String, Object> props = new HashMap<>();
        props.put("copy.field", oldField);
        props.put("new.field", newField);
        xform.configure(props);

        final Schema sourceFieldSchema = SchemaBuilder.struct().name("sourceField").version(1).doc("doc")
                .field("name",Schema.OPTIONAL_STRING_SCHEMA)
                .field("db",Schema.OPTIONAL_STRING_SCHEMA)
                .field("table",Schema.OPTIONAL_STRING_SCHEMA);
        final Struct sourceFieldValue = new Struct(sourceFieldSchema)
                .put("name","fullfillment")
                .put("db","TutorialDB")
                .put("table","Employees");

        final Schema stateFieldSchema = SchemaBuilder.struct().name("before").version(1).doc("doc")
                .field("name",Schema.OPTIONAL_STRING_SCHEMA)
                .field("age",Schema.OPTIONAL_STRING_SCHEMA);
        final Struct beforeValue = new Struct(stateFieldSchema)
                .put("name","John")
                .put("age","23");
        final Struct afterValue = new Struct(stateFieldSchema)
                .put("name","Aya")
                .put("age","24");

        final Schema valueSchema = SchemaBuilder.struct().name("name").version(1).doc("doc")
                .field("before",stateFieldSchema)
                .field("after",stateFieldSchema)
                .field("source",sourceFieldSchema)
                .field("op",Schema.OPTIONAL_STRING_SCHEMA)
                .field("ts_ms",Schema.OPTIONAL_STRING_SCHEMA)
                .field("requestAppId",Schema.OPTIONAL_STRING_SCHEMA);
        final Struct value = new Struct(valueSchema)
                .put("before",beforeValue)
                .put("after",afterValue)
                .put("source",sourceFieldValue)
                .put("op","d")
                .put("ts_ms", "1650444582336")
                .put("requestAppId","Kafka");



        final SourceRecord employee = new SourceRecord(null, null, "test", 0, valueSchema, value);
        final SourceRecord transformedEmployee = xform.apply(employee);

        assertEquals("d", ((Struct) transformedEmployee.value()).get("op"));
        assertEquals(employee.valueSchema(), transformedEmployee.valueSchema());
        assertEquals(((Struct) employee.value()).get("before"), ((Struct) transformedEmployee.value()).get("after"));

    }

    @Test
    public void schemaless() {
        final String oldField = "before";
        final String newField = "after";
        final Map<String, Object> props = new HashMap<>();
        props.put("copy.field", oldField);
        props.put("new.field", newField);
        xform.configure(props);
//        final SourceRecord record = new SourceRecord(null, null, "test", 0,
//                null, Collections.singletonMap("op", "c"));
//        final SourceRecord transformedRecord = xform.apply(record);
    }

}