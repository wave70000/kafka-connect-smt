package org.wave7.kafka;

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

public abstract class ConvertOperation<R extends ConnectRecord<R>> implements Transformation<R> {
    public static final String OVERVIEW_DOC =
            "Convert Operation type";
    private interface ConfigName {
        String OP_FIELD_NAME = "operation.field.name";
    }
    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.OP_FIELD_NAME, ConfigDef.Type.STRING, "op", ConfigDef.Importance.HIGH,
                    "Field name for Operation Type");
    private static final String PURPOSE = "convert operation type word";
    private String fieldName;
    private Cache<Schema, Schema> schemaUpdateCache;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(ConfigName.OP_FIELD_NAME);

        schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
    }

    @Override
    public R apply(R record) {
        final Schema schema = operatingSchema(record);
        System.out.println(schema);
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        if (record.value() != null){
            final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
            final Map<String, Object> updatedValue = new HashMap<>(value);

            updatedValue.put("ReqType", getOperation(value.get("op")));

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
            if (field.name().contains(fieldName)){
                updatedValue.put("ReqType", getOperation(value.get(field)));
            }
        }

//        updatedValue.put("ReqType", "TestSchema");
        return newRecord(record, updatedSchema, updatedValue);
    }

    private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());
        for (Field field: schema.fields()) {
            builder.field(field.name(), field.schema());
        }
        builder.field("ReqType", Schema.OPTIONAL_STRING_SCHEMA);
        return builder.build();
    }

    private String getOperation(Object value) {
        if (value == null) {
            return null;
        } else if (value == "c") {
            return "Add";
        } else if (value == "r") {
            return "Add";
        } else if (value == "u") {
            return "Edit";
        } else if (value == "d") {
            return "Delete";
        }
        return "Not match";
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
    public static class Key<R extends ConnectRecord<R>> extends ConvertOperation<R> {
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

    public static class Value<R extends ConnectRecord<R>> extends ConvertOperation<R> {
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
