package com.mfec.cpm.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class CopyField<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Copy One Field to Another Field";
    private interface ConfigName {
        String FIELD_NAME = "copy.field";
        String NEW_FIELD_NAME = "new.field";
        String OP_FIELD_NAME = "op.field";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.FIELD_NAME, ConfigDef.Type.STRING,"field", ConfigDef.Importance.HIGH,
                    "Field to copied")
            .define(ConfigName.NEW_FIELD_NAME, ConfigDef.Type.STRING, "new_field", ConfigDef.Importance.HIGH,
                    "New field")
            .define(ConfigName.OP_FIELD_NAME, ConfigDef.Type.STRING, "op", ConfigDef.Importance.HIGH,
                    "Operation field");

    private static final String PURPOSE = "Copy value from one field to another";
    private String fieldName;
    private String newFieldName;
    private String operationField;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(ConfigName.FIELD_NAME);
        newFieldName = config.getString(ConfigName.NEW_FIELD_NAME);
        operationField = config.getString(ConfigName.OP_FIELD_NAME);

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
            final Map<String, Object> updatedValue = new HashMap<>(value);

            updatedValue.put(newFieldName, value.get(fieldName));

            return newRecord(record, updatedValue);
        }
        return null;
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        final Struct updatedValue = new Struct(value.schema());

        for (Field field : value.schema().fields()) {
            final Object originalValue = value.get(field);
            updatedValue.put(field.name(), originalValue);
            if (field.name().equals(newFieldName) && value.get(operationField) == "d") {
                updatedValue.put(newFieldName,value.get(fieldName));
            }
        }

        return newRecord(record, updatedValue);
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }

    protected abstract Schema operatingSchema(R record);
    protected abstract Object operatingValue(R record);
    protected abstract R newRecord(R record, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends CopyField<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }
        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }
        @Override
        protected R newRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), updatedValue, record.timestamp());
        }
    }

    public static class Value<R extends ConnectRecord<R>> extends CopyField<R> {
        @Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }
        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }
        @Override
        protected R newRecord(R record, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), record.valueSchema(), updatedValue, record.timestamp());
        }
    }
}
