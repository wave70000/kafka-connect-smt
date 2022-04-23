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

import static org.junit.Assert.*;

public class convertOperationTest {

    private ConvertOperation<SourceRecord> xform = new ConvertOperation.Value<>();

    @After
    public void tearDown() throws Exception {
        xform.close();
    }

    @Test(expected = DataException.class)
    public void topLevelStructRequired() {
        xform.configure(Collections.singletonMap("operation.field", "op"));
        xform.configure(Collections.singletonMap("new.field", "reqType"));
        xform.configure(Collections.singletonMap("convert.mapping", "c:Add,r:Add,u:Edit,d:Delete"));
        xform.apply(new SourceRecord(null, null, "", 0, Schema.INT32_SCHEMA, 42));
    }

    @Test
    public void copySchemaAndInsertUuidField() {
        final String operationField = "op";
        final String newFieldName = "reqType";
        final String mappingField = "c:Add,r:Add,u:Edit,d:Delete";
        final Map<String, Object> props = new HashMap<>();
        props.put("operation.field", operationField);
        props.put("new.field", newFieldName);
        props.put("convert.mapping", mappingField);
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

        assertEquals("c", ((Struct) transformedEmployee.value()).get(operationField));
        assertEquals("Add", ((Struct) transformedEmployee.value()).get(newFieldName));

//        assertEquals(simpleStructSchema.name(), transformRecordMssql.valueSchema().name());
//        assertEquals(simpleStructSchema.version(), transformRecordMssql.valueSchema().version());
//        assertEquals(simpleStructSchema.doc(), transformRecordMssql.valueSchema().doc());
//        assertEquals(Schema.OPTIONAL_STRING_SCHEMA, transformRecordMssql.valueSchema().field("op").schema());


    }

    @Test
    public void schemalessInsertUuidField() {
//        final Map<String, Object> props = new HashMap<>();
//        props.put("operation.field.name", "op");
//        xform.configure(props);
//        final SourceRecord record = new SourceRecord(null, null, "test", 0,
//                null, Collections.singletonMap("op", "c"));
//        final SourceRecord transformedRecord = xform.apply(record);
    }
}