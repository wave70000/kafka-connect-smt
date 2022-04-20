package org.wave7.kafka;


import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.NonEmptyListValidator;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class MaskOperation<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "AAA";

    private interface ConfigName {
        String OP_FIELD_NAME = "operation.field.name";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.OP_FIELD_NAME, ConfigDef.Type.STRING, "op",
                    ConfigDef.Importance.HIGH, "Names of fields to mask.");

    private static final String PURPOSE = "mask fields";

    private static final Map<Class<?>, Function<String, ?>> REPLACEMENT_MAPPING_FUNC = new HashMap<>();
    private static final Map<Class<?>, Object> PRIMITIVE_VALUE_MAPPING = new HashMap<>();

    static {
        PRIMITIVE_VALUE_MAPPING.put(Boolean.class, Boolean.FALSE);
        PRIMITIVE_VALUE_MAPPING.put(Byte.class, (byte) 0);
        PRIMITIVE_VALUE_MAPPING.put(Short.class, (short) 0);
        PRIMITIVE_VALUE_MAPPING.put(Integer.class, 0);
        PRIMITIVE_VALUE_MAPPING.put(Long.class, 0L);
        PRIMITIVE_VALUE_MAPPING.put(Float.class, 0f);
        PRIMITIVE_VALUE_MAPPING.put(Double.class, 0d);
        PRIMITIVE_VALUE_MAPPING.put(BigInteger.class, BigInteger.ZERO);
        PRIMITIVE_VALUE_MAPPING.put(BigDecimal.class, BigDecimal.ZERO);
        PRIMITIVE_VALUE_MAPPING.put(Date.class, new Date(0));
        PRIMITIVE_VALUE_MAPPING.put(String.class, "");

        REPLACEMENT_MAPPING_FUNC.put(Byte.class, v -> Values.convertToByte(null, v));
        REPLACEMENT_MAPPING_FUNC.put(Short.class, v -> Values.convertToShort(null, v));
        REPLACEMENT_MAPPING_FUNC.put(Integer.class, v -> Values.convertToInteger(null, v));
        REPLACEMENT_MAPPING_FUNC.put(Long.class, v -> Values.convertToLong(null, v));
        REPLACEMENT_MAPPING_FUNC.put(Float.class, v -> Values.convertToFloat(null, v));
        REPLACEMENT_MAPPING_FUNC.put(Double.class, v -> Values.convertToDouble(null, v));
        REPLACEMENT_MAPPING_FUNC.put(String.class, Function.identity());
        REPLACEMENT_MAPPING_FUNC.put(BigDecimal.class, BigDecimal::new);
        REPLACEMENT_MAPPING_FUNC.put(BigInteger.class, BigInteger::new);
    }

    private String fieldName;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(ConfigName.OP_FIELD_NAME);
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
        System.out.println(record);
        if (record.value() != null) {
            final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);
            final HashMap<String, Object> updatedValue = new HashMap<>(value);

            updatedValue.put(fieldName, masked(value.get(fieldName)));

            return newRecord(record, updatedValue);
        }
        return null;
    }

    private R applyWithSchema(R record) {
        System.out.println(record);
        final Struct value = requireStruct(operatingValue(record), PURPOSE);
        final Struct updatedValue = new Struct(value.schema());
        for (Field field : value.schema().fields()) {
            final Object origFieldValue = value.get(field);
            updatedValue.put(field, fieldName.contains(field.name()) ? masked(origFieldValue) : origFieldValue);
        }
        return newRecord(record, updatedValue);
    }

    private Object masked(Object value) {
        if (value == null) {
//            System.out.println("found null");
            return null;
        }
//        return maskWithCustomReplacement(value, replacement);
        return convertOperation(value);
    }

    private static Object convertOperation(Object value) {
//        System.out.println(REPLACEMENT_MAPPING_FUNC.get(value.getClass()));
//        Function<String, ?> replacementMapper = REPLACEMENT_MAPPING_FUNC.get(value.getClass());
//        if (replacementMapper == null) {
//            throw new DataException("Cannot mask value of type " + value.getClass() + " with custom replacement.");
//        }
//        try {
//            if (value.toString().equals("r")) {
//                System.out.println(replacementMapper.apply("Add"));
//                return replacementMapper.apply("Add");
//            } else if (value.toString().equals("c")) {
//                System.out.println(replacementMapper.apply("Add"));
//                return replacementMapper.apply("Add");
//            } else if (value.toString().equals("u")) {
//                System.out.println(replacementMapper.apply("Edit"));
//                return replacementMapper.apply("Edit");
//            }
//                System.out.println(replacementMapper.apply("Delete"));
//                return replacementMapper.apply("Delete");
//        } catch (NumberFormatException ex) {
//            throw new DataException("Unable to convert");
//        }
        try {
            if (value.toString().equals("r")) {
                System.out.println("Add");
                return "Add";
            } else if (value.toString().equals("c")) {
                System.out.println("Add");
                return "Add";
            } else if (value.toString().equals("u")) {
                System.out.println("Edit");
                return "Edit";
            } else if (value.toString().equals("d")) {
                System.out.println("Delete");
                return "Delete";
            }
            return "Not Match";
        } catch (NumberFormatException ex) {
            throw new DataException("Unable to convert");
        }

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

    public static final class Key<R extends ConnectRecord<R>> extends MaskOperation<R> {
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

    public static final class Value<R extends ConnectRecord<R>> extends MaskOperation<R> {
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
