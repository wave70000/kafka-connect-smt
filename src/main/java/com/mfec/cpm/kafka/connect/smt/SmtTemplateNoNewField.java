package com.mfec.cpm.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class SmtTemplateNoNewField<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC = "";

    private interface ConfigName {
        //TODO: ADD PROPERTIES
        String FIELD_NAME = "field.name";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            //TODO: ADD CONFIG
            .define(ConfigName.FIELD_NAME, ConfigDef.Type.STRING, "field",
                    ConfigDef.Importance.HIGH, "Field");

    private static final String PURPOSE = "purpose";
    //TODO: ADD VARIABLE
    private String fieldName;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(ConfigName.FIELD_NAME);
    }

    @Override
    public R apply(R record) {
        if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        if (record.value() != null) {
            final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
            final HashMap<String, Object> updatedValue = new HashMap<>(value);

            //TODO: ADD LOGIC TO RECORD WITH SCHEMALESS STRUCTURE
//            updatedValue.put(fieldName, value.get(fieldName));

            return newRecord(record, updatedValue);
        }
        return null;
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        final Struct updatedValue = new Struct(value.schema());

        //TODO: ADD LOGIC TO DATA WITH SCHEMA STRUCTURE
        for (Field field : value.schema().fields()) {
            final Object origFieldValue = value.get(field);
//            updatedValue.put(fieldName, origFieldValue);
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

    protected abstract R newRecord(R base, Object value);

    public static final class Key<R extends ConnectRecord<R>> extends SmtTemplateNoNewField<R> {
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
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }
    }

    public static final class Value<R extends ConnectRecord<R>> extends SmtTemplateNoNewField<R> {
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
