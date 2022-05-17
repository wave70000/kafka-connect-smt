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

public class CastNestedFieldTest {

    private CastNestedField<SourceRecord> xform = new CastNestedField.Value<>();

    @After
    public void tearDown() throws Exception {
        xform.close();
    }

    @Test(expected = DataException.class)
    public void topLevelStructRequired() {
        xform.configure(Collections.singletonMap("struct.name", "param"));
        xform.configure(Collections.singletonMap("list.field", "IsInactive:invertCastBoolean,isDeleted:castBoolean"));
        xform.apply(new SourceRecord(null, null, "", 0, Schema.INT32_SCHEMA, 1));

    }

    @Test
    public void testSchema() {
        final Map<String, Object> props = new HashMap<>();
        //TODO: ADD PROPERTIES
        props.put("struct.name", "param");
        props.put("list.field", "IsInactive:invertCastBoolean,isDeleted:castBoolean,FixDate:castBoolean,IsClosyear:castBoolean");

        xform.configure(props);

        //TODO: CREATE STRUCTURE AND VALUE
        final Schema sourceFieldSchema = SchemaBuilder.struct().name("sourceField").version(1).doc("doc")
                .field("name",Schema.OPTIONAL_STRING_SCHEMA)
                .field("db",Schema.OPTIONAL_STRING_SCHEMA)
                .field("table",Schema.OPTIONAL_STRING_SCHEMA);
        final Struct sourceFieldValue = new Struct(sourceFieldSchema)
                .put("name","fullfillment")
                .put("db","TutorialDB")
                .put("table","Employees");

        final Schema paramFieldSchema = SchemaBuilder.struct().name("paramField").version(1).doc("doc")
                .field("IsInactive",Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .field("isDeleted",Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .field("FixDate",Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .field("IsClosyear",Schema.OPTIONAL_BOOLEAN_SCHEMA)
                .field("testField",Schema.OPTIONAL_STRING_SCHEMA);
        final Struct paramFieldValue = new Struct(paramFieldSchema)
                .put("IsInactive",false)
                .put("isDeleted",true)
                .put("FixDate",true)
                .put("IsClosyear",false)
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


        assertEquals(false,((Struct)employee.value()).getStruct("param").get("IsInactive"));
        assertEquals(Boolean.class,((Struct) employee.value()).getStruct("param").get("IsInactive").getClass());
        assertEquals(true,((Struct)employee.value()).getStruct("param").get("isDeleted"));
        assertEquals(Boolean.class,((Struct) employee.value()).getStruct("param").get("isDeleted").getClass());
        assertEquals(true,((Struct)employee.value()).getStruct("param").get("FixDate"));
        assertEquals(Boolean.class,((Struct) employee.value()).getStruct("param").get("FixDate").getClass());
        assertEquals(false,((Struct)employee.value()).getStruct("param").get("IsClosyear"));
        assertEquals(Boolean.class,((Struct) employee.value()).getStruct("param").get("IsClosyear").getClass());
        assertEquals("false",((Struct)employee.value()).getStruct("param").get("testField"));
        assertEquals(String.class,((Struct) employee.value()).getStruct("param").get("testField").getClass());

        assertEquals("1",((Struct)transformedEmployee.value()).getStruct("param").get("IsInactive"));
        assertEquals(String.class,((Struct) transformedEmployee.value()).getStruct("param").get("IsInactive").getClass());
        assertEquals("1",((Struct)transformedEmployee.value()).getStruct("param").get("isDeleted"));
        assertEquals(String.class,((Struct) transformedEmployee.value()).getStruct("param").get("isDeleted").getClass());
        assertEquals("1",((Struct)transformedEmployee.value()).getStruct("param").get("FixDate"));
        assertEquals(String.class,((Struct) transformedEmployee.value()).getStruct("param").get("FixDate").getClass());
        assertEquals("0",((Struct)transformedEmployee.value()).getStruct("param").get("IsClosyear"));
        assertEquals(String.class,((Struct) transformedEmployee.value()).getStruct("param").get("IsClosyear").getClass());
        assertEquals("false",((Struct)transformedEmployee.value()).getStruct("param").get("testField"));
        assertEquals(String.class,((Struct) transformedEmployee.value()).getStruct("param").get("testField").getClass());
    }

    @Test
    public void testSchemaless() {

    }

}