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
import static org.junit.Assert.assertNotNull;

public class ExtractFieldFromStructTest {

    private ExtractFieldFromStruct<SourceRecord> xform = new ExtractFieldFromStruct.Value<>();

    @After
    public void tearDown() throws Exception {
        xform.close();
    }

    @Test(expected = DataException.class)
    public void topLevelStructRequired() {
        xform.configure(Collections.singletonMap("field", "table"));
        xform.configure(Collections.singletonMap("struct.field", "source"));
        xform.configure(Collections.singletonMap("new.field", "tableName"));

        xform.apply(new SourceRecord(null, null, "test", 0, Schema.INT32_SCHEMA, 42));
    }

    @Test
    public void copySchemaAndExtractField() {
        final Map<String, Object> props = new HashMap<>();
        props.put("field", "table");
        props.put("struct.field", "source");
        props.put("new.field", "tableName");
        xform.configure(props);

        final Schema sourceFieldSchema = SchemaBuilder.struct().name("sourceField").version(1).doc("doc")
                .field("name",Schema.OPTIONAL_STRING_SCHEMA)
                .field("db",Schema.OPTIONAL_STRING_SCHEMA)
                .field("table",Schema.OPTIONAL_STRING_SCHEMA);
        final Schema valueSchema = SchemaBuilder.struct().name("name").version(1).doc("doc")
                .field("param",Schema.OPTIONAL_STRING_SCHEMA)
                .field("source",sourceFieldSchema)
                .field("op",Schema.OPTIONAL_STRING_SCHEMA)
                .field("ts_ms",Schema.OPTIONAL_STRING_SCHEMA)
                .field("requestAppId",Schema.OPTIONAL_STRING_SCHEMA);

        final Struct sourceFieldValue = new Struct(sourceFieldSchema)
                .put("name","fullfillment")
                .put("db","TutorialDB")
                .put("table","Employees");
        final Struct value = new Struct(valueSchema)
                .put("param","testParam")
                .put("source",sourceFieldValue)
                .put("op","c")
                .put("ts_ms", "1650444582336")
                .put("requestAppId","HRMI");

        final SourceRecord employee = new SourceRecord(null, null, "test", 0, valueSchema, value);
        final SourceRecord transformedEmployee = xform.apply(employee);
        System.out.println(transformedEmployee);

        assertEquals("Employees", ((Struct) transformedEmployee.value()).getStruct("source").get("table"));
        assertEquals("Employees", ((Struct) transformedEmployee.value()).get("tableName"));
        assertEquals("tableName", transformedEmployee.valueSchema().field("tableName").name());

    }

    @Test
    public void schemalessInsertUuidField() {

    }
}