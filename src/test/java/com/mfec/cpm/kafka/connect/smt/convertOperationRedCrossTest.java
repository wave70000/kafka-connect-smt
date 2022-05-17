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

public class convertOperationRedCrossTest {

    private ConvertOperationRedCross<SourceRecord> xform = new ConvertOperationRedCross.Value<>();

    @After
    public void tearDown() throws Exception {
        xform.close();
    }

    @Test(expected = DataException.class)
    public void topLevelStructRequired() {
        xform.configure(Collections.singletonMap("operation.field", "op"));
        xform.configure(Collections.singletonMap("new.field", "reqType"));
        xform.configure(Collections.singletonMap("convert.mapping", "c:Add,r:Add,u:Edit,d:Delete"));
        xform.configure(Collections.singletonMap("delete.field", "IsDeleted"));

        xform.apply(new SourceRecord(null, null, "", 0, Schema.INT32_SCHEMA, 42));
    }

    @Test
    public void copySchemaAndInsertUuidField() {
        final String operationField = "op";
        final String newFieldName = "reqType";
        final String mappingField = "c:Add,r:Add,u:Edit,d:Delete";
        final String deleteField = "IsDeleted";
        final Map<String, Object> props = new HashMap<>();
        props.put("operation.field", operationField);
        props.put("new.field", newFieldName);
        props.put("convert.mapping", mappingField);
        props.put("delete.field", deleteField);
        xform.configure(props);

        final Schema sourceFieldSchema = SchemaBuilder.struct().name("sourceField").version(1).doc("doc")
                .field("name",Schema.OPTIONAL_STRING_SCHEMA)
                .field("db",Schema.OPTIONAL_STRING_SCHEMA)
                .field("table",Schema.OPTIONAL_STRING_SCHEMA);
        final Struct sourceFieldValue = new Struct(sourceFieldSchema)
                .put("name","fullfillment")
                .put("db","TutorialDB")
                .put("table","Employees");

        final Schema paramFieldSchema = SchemaBuilder.struct().name("paramField").version(1).doc("doc")
                .field("IsInactive",Schema.OPTIONAL_STRING_SCHEMA)
                .field("IsDeleted",Schema.OPTIONAL_STRING_SCHEMA)
                .field("FixDate",Schema.OPTIONAL_STRING_SCHEMA)
                .field("IsClosyear",Schema.OPTIONAL_STRING_SCHEMA)
                .field("testField",Schema.OPTIONAL_STRING_SCHEMA);
        final Struct paramFieldValue = new Struct(paramFieldSchema)
                .put("IsInactive","1")
                .put("IsDeleted","1")
                .put("FixDate","1")
                .put("IsClosyear","1")
                .put("testField","false");

        final Schema valueSchema = SchemaBuilder.struct().name("name").version(1).doc("doc")
                .field("param",paramFieldSchema)
                .field("source",sourceFieldSchema)
                .field("op",Schema.OPTIONAL_STRING_SCHEMA)
                .field("ts_ms",Schema.OPTIONAL_STRING_SCHEMA)
                .field("requestAppId",Schema.OPTIONAL_STRING_SCHEMA);
        final Struct value = new Struct(valueSchema)
                .put("param",paramFieldValue)
                .put("source",sourceFieldValue)
                .put("op","c")
                .put("ts_ms", "1650444582336")
                .put("requestAppId","Kafka");
        final SourceRecord employee = new SourceRecord(null, null, "test", 0, valueSchema, value);
        final SourceRecord transformedEmployee = xform.apply(employee);

        assertEquals("1", ((Struct)employee.value()).getStruct("param").get("IsDeleted"));
        assertEquals("1", ((Struct)transformedEmployee.value()).getStruct("param").get("IsDeleted"));
        assertEquals("c", ((Struct) employee.value()).get(operationField));
        assertEquals("c", ((Struct) transformedEmployee.value()).get(operationField));

        assertEquals("Delete", ((Struct) transformedEmployee.value()).get(newFieldName));



    }

    @Test
    public void schemalessInsertUuidField() {

    }
}