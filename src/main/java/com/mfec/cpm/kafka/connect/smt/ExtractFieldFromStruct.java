package com.mfec.cpm.kafka.connect.smt;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class ExtractFieldFromStruct<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Extract single field from Struct";
    private interface ConfigName {
        String FIELD_NAME = "field";
        String STRUCT_NAME = "struct.field";
        String NEW_FIELD_NAME = "new.field";
    }
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.FIELD_NAME, ConfigDef.Type.STRING, "field",ConfigDef.Importance.HIGH,
                    "Field name for Extraction")
            .define(ConfigName.STRUCT_NAME, ConfigDef.Type.STRING, "field", ConfigDef.Importance.HIGH,
                    "Struct field name")
            .define(ConfigName.NEW_FIELD_NAME, ConfigDef.Type.STRING, "new_field", ConfigDef.Importance.HIGH,
                    "New field name");
    private static final String PURPOSE = "extract field from struct";
    private String fieldName;
    private String structName;
    private String newFieldName;
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(ConfigName.FIELD_NAME);
        structName = config.getString(ConfigName.STRUCT_NAME);
        newFieldName = config.getString(ConfigName.NEW_FIELD_NAME);

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
        if (record.value() != null){
            final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
            final Map<String, Object> updatedValue = new HashMap<>(value);;
            //TODO: Implement schemaless function
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
            updatedValue.put(field.name(), value.get(field));
            if (field.name().contains(structName)){
                updatedValue.put(newFieldName, value.getStruct(structName).get(fieldName));
            }
        }

        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field: schema.fields()) {
            builder.field(field.name(), field.schema());
        }
        builder.field(newFieldName, Schema.OPTIONAL_STRING_SCHEMA);
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
    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends ExtractFieldFromStruct<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }
        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }
        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends ExtractFieldFromStruct<R> {
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
