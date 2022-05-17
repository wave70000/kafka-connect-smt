package com.mfec.cpm.kafka.connect.smt;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.*;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class CastNestedField<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "Cast Nested Field";

    private interface ConfigName {
        //TODO: ADD PROPERTIES
        String STRUCT_NAME = "struct.name";
        String LIST_FIELD = "list.field";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            //TODO: ADD CONFIG
            .define(ConfigName.STRUCT_NAME, ConfigDef.Type.STRING, "field",
                    ConfigDef.Importance.HIGH, "Field")
            .define(ConfigName.LIST_FIELD, ConfigDef.Type.LIST, Collections.EMPTY_LIST,
                    ConfigDef.Importance.HIGH, "List casted field");

    private static final String PURPOSE = "cast nested field";
    //TODO: ADD VARIABLE
    private String fieldName;
    private Map<String,String> listField;
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(ConfigName.STRUCT_NAME);
        listField = parseMapping(config.getList(ConfigName.LIST_FIELD));

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        final Schema schema = operatingSchema(record);
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        if (record.value() != null) {
            final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
            final Map<String, Object> updatedValue = new HashMap<>(value);

            //TODO: ADD LOGIC TO RECORD WITH SCHEMALESS STRUCTURE
            updatedValue.put(fieldName, value.get(fieldName));

            return newRecord(record, null, updatedValue);
        }
        return null;
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if(updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            final Object origFieldValue = value.get(field);

            if (field.name().contains(fieldName)) {
                final Struct valueInStruct = requireStruct(value.getStruct(fieldName), PURPOSE);
                final Struct updateValueInStruct = new Struct(updatedSchema.field(fieldName).schema());

                for (Field fieldInStruct : valueInStruct.schema().fields()) {
                    final String fieldInStructName = fieldInStruct.name();
                    final Object origFieldValueInStruct = valueInStruct.get(fieldInStruct);

                    if (listField.containsKey(fieldInStructName)) {
                        if (origFieldValueInStruct.getClass() != Boolean.class) {
                            throw new DataException("Can not cast " + origFieldValueInStruct.getClass());
                        }
                        String valueMapping = getMapping(fieldInStructName);
                        getFunction((Boolean) origFieldValueInStruct, valueMapping);
                        Field newNestedField = updateValueInStruct.schema().field(fieldInStructName);
                        updateValueInStruct.put(newNestedField, getFunction((Boolean) origFieldValueInStruct, valueMapping));
                    } else {
                        updateValueInStruct.put(fieldInStruct,origFieldValueInStruct);
                    }
                }
                //TODO test
                Field newStructField = updatedSchema.schema().field(field.name());
                updatedValue.put(newStructField, updateValueInStruct);
            } else {
                updatedValue.put(field, origFieldValue);
            }
        }
        return newRecord(record, updatedSchema, updatedValue);
    }

    static Map<String,String> parseMapping(List<String> mappings) {
        final Map<String, String> m = new HashMap<>();
        for(String mapping : mappings) {
            final String[] parts = mapping.split(":");
            if (parts.length != 2) {
                throw new ConfigException(ConfigName.LIST_FIELD, mappings, "Invalid Mapping");
            }
            m.put(parts[0],parts[1]);
        }
        return m;
    }
    private String getMapping(Object value) {
        if (value == null) {
            return null;
        }
        return listField.getOrDefault(value, null);
    }

    static String castBoolean(Boolean value) {
        return value ? "1" : "0";
    }

    static String invertCastBoolean(Boolean value) {
        return value ? "0" : "1";
    }

    static String getFunction(Boolean value,String function) {
        switch (function) {
            case "castBoolean":
                return castBoolean(value);
            case "invertCastBoolean":
                return invertCastBoolean(value);
        }
        return null;
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field: schema.fields()) {

            if (field.name().equals(fieldName)) {
                final SchemaBuilder structBuilder = SchemaUtil.copySchemaBasics(field.schema(), SchemaBuilder.struct());
                for (Field fieldInStruct : field.schema().fields()) {
                    String fieldInStructName = fieldInStruct.name();
                    if (listField.containsKey(fieldInStructName)) {
                        structBuilder.field(fieldInStructName, Schema.OPTIONAL_STRING_SCHEMA);
                    } else
                        structBuilder.field(fieldInStructName, fieldInStruct.schema());
                }
                builder.field(fieldName, structBuilder);
            } else {
                builder.field(field.name(), field.schema());
            }
        }
        return builder.build();
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        schemaUpdateCache = null;
    }

    protected abstract Schema operatingSchema(R record);
    protected abstract Object operatingValue(R record);
    protected abstract R newRecord(R record, Schema updatedSchema, Object value);
    public static class Key<R extends ConnectRecord<R>> extends CastNestedField<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }
        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }
        @Override
        protected R newRecord(R record,Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }
    public static class Value<R extends ConnectRecord<R>> extends CastNestedField<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }
        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }
        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }
    }
}
