package org.neolumin.simpleOrm;

import org.json.JSONObject;

import java.io.*;
import java.lang.reflect.Constructor;
import java.nio.ByteBuffer;
import java.util.*;

class ModelMetadata<T> {
    public static final String DEFAULT_DISCRIMINATOR = "";
    private final Class entityRowClass;
    private final String discriminatorColumnFamily;
    private final String discriminatorColumnName;
    private final String tableName;
    private final IdField idField;
    private final Map<String, Type> types;
    private static final Map<Class, ModelMetadata> rowClassMetadata = new HashMap<>();

    public ModelMetadata(
            Class entityRowClass,
            IdField idField,
            Map<String, Type> types,
            String discriminatorColumnFamily,
            String discriminatorColumnName,
            String tableName
    ) {
        this.entityRowClass = entityRowClass;
        this.idField = idField;
        this.types = types;
        this.discriminatorColumnFamily = discriminatorColumnFamily;
        this.discriminatorColumnName = discriminatorColumnName;
        this.tableName = tableName;
    }

    public String getDiscriminatorColumnName() {
        return discriminatorColumnName;
    }

    public String getDiscriminatorColumnFamily() {
        return discriminatorColumnFamily;
    }

    public String getTableName() {
        return tableName;
    }

    public Class getEntityRowClass() {
        return entityRowClass;
    }

    public Collection<Type> getTypes() {
        return this.types.values();
    }

    public Set<Field> getFields() {
        Set<Field> fields = new HashSet<>();
        for (Type t : getTypes()) {
            for (Field field : t.getAllFields()) {
                fields.add(field);
            }
        }
        return fields;
    }

    public Type getType(String discriminatorValue) {
        if (discriminatorValue == null) {
            discriminatorValue = DEFAULT_DISCRIMINATOR;
        }
        return types.get(discriminatorValue);
    }

    public static <T> ModelMetadata<T> getModelMetadata(Class<T> rowClass) {
        //noinspection unchecked
        ModelMetadata<T> metadata = rowClassMetadata.get(rowClass);
        if (metadata == null) {
            Class c = rowClass.getSuperclass();
            while (c != null && metadata == null) {
                //noinspection unchecked
                metadata = rowClassMetadata.get(c);
                c = c.getSuperclass();
            }
        }
        if (metadata != null) {
            return metadata;
        }
        metadata = ModelMetadataBuilder.build(rowClass);
        rowClassMetadata.put(metadata.getEntityRowClass(), metadata);
        return metadata;
    }

    public static <T> ModelMetadata<T> getModelMetadata(T obj) {
        //noinspection unchecked
        return (ModelMetadata<T>) ModelMetadata.getModelMetadata(obj.getClass());
    }

    public Type getTypeFromObject(T obj) {
        for (Type t : this.types.values()) {
            if (t.getRowClass().equals(obj.getClass())) {
                return t;
            }
        }
        return null;
    }

    public void setIdField(Object obj, String rowKey) {
        if (idField == null) {
            return;
        }
        idField.set(obj, rowKey);
    }

    public String getId(Object obj) {
        if (idField == null) {
            return null;
        }
        return idField.getString(obj);
    }

    public static class Type {
        private final Class rowClass;
        private final Constructor constructor;
        private final Map<String, Map<String, Field>> fields;

        public Type(Class rowClass, Constructor constructor, Map<String, Map<String, Field>> fields) {
            this.rowClass = rowClass;
            this.constructor = constructor;
            this.constructor.setAccessible(true);
            this.fields = fields;
        }

        public Class getRowClass() {
            return rowClass;
        }

        public Constructor getConstructor() {
            return constructor;
        }

        public Map<String, Map<String, Field>> getFields() {
            return fields;
        }

        public <T> T newInstance() {
            try {
                //noinspection unchecked
                return (T) getConstructor().newInstance();
            } catch (Exception e) {
                throw new RuntimeException("Could not create class: " + getRowClass().getName());
            }
        }

        public Field getFieldForColumn(String columnFamily, String columnName) {
            Map<String, Field> columnFamilyFields = this.fields.get(columnFamily);
            if (columnFamilyFields == null) {
                return null;
            }
            return columnFamilyFields.get(columnName);
        }

        public Collection<Field> getAllFields() {
            List<Field> fields = new ArrayList<>();
            for (Map<String, Field> fieldsForColumnFamily : getFields().values()) {
                for (Field field : fieldsForColumnFamily.values()) {
                    fields.add(field);
                }
            }
            return fields;
        }
    }

    public abstract static class Field<T> {
        private final java.lang.reflect.Field field;
        private final String columnFamily;
        private final String columnName;

        public Field(java.lang.reflect.Field field, String columnFamily, String columnName) {
            this.field = field;
            this.columnFamily = columnFamily;
            this.columnName = columnName;
            this.field.setAccessible(true);
        }

        public String getColumnFamily() {
            return columnFamily;
        }

        public String getColumnName() {
            return columnName;
        }

        public void set(Object o, byte[] value) {
            try {
                getField().set(o, valueToJava(value));
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Could not set field: " + getField().getName() + " on class " + o.getClass().getName());
            }
        }

        protected T getRaw(Object obj) {
            try {
                //noinspection unchecked
                return (T) getField().get(obj);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Could not get field value: " + getField().getName() + " on class " + obj.getClass().getName());
            }
        }

        protected java.lang.reflect.Field getField() {
            return field;
        }

        protected abstract T valueToJava(byte[] value);

        public final byte[] get(Object obj) {
            T rawValue = getRaw(obj);
            if (rawValue == null) {
                return null;
            }
            return javaToByteArray(rawValue);
        }

        protected abstract byte[] javaToByteArray(T value);

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Field other = (Field) o;

            if (columnFamily != null ? !columnFamily.equals(other.columnFamily) : other.columnFamily != null) {
                return false;
            }
            if (columnName != null ? !columnName.equals(other.columnName) : other.columnName != null) {
                return false;
            }
            return true;
        }

        @Override
        public int hashCode() {
            return (getColumnFamily() != null ? getColumnFamily().hashCode() : 0)
                    ^ (getColumnName() != null ? getColumnName().hashCode() : 0);
        }

        public void setRaw(Object obj, T value) {
            try {
                getField().set(obj, value);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Could not set field: " + getField().getName(), e);
            }
        }
    }

    public static class StringField extends Field<String> {
        public StringField(java.lang.reflect.Field field, String columnFamily, String columnName) {
            super(field, columnFamily, columnName);
        }

        @Override
        protected String valueToJava(byte[] value) {
            return new String(value);
        }

        @Override
        protected byte[] javaToByteArray(String value) {
            return value.getBytes();
        }
    }

    public static class IdField extends StringField {
        public IdField(java.lang.reflect.Field field) {
            super(field, null, null);
        }

        public void set(Object o, String value) {
            try {
                getField().set(o, value);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Could not set field: " + getField().getName() + " on class " + o.getClass().getName());
            }
        }

        public String getString(Object obj) {
            return getRaw(obj);
        }
    }

    public static class EnumField extends Field<Enum> {
        public EnumField(java.lang.reflect.Field field, String columnFamily, String columnName) {
            super(field, columnFamily, columnName);
        }

        @Override
        protected Enum valueToJava(byte[] value) {
            String str = new String(value);
            Class fieldType = (Class) getField().getType();
            return Enum.valueOf(fieldType, str);
        }

        @Override
        protected byte[] javaToByteArray(Enum value) {
            return value.name().getBytes();
        }
    }

    public static class DateField extends Field<Date> {
        public DateField(java.lang.reflect.Field field, String columnFamily, String columnName) {
            super(field, columnFamily, columnName);
        }

        @Override
        protected Date valueToJava(byte[] value) {
            long time = ByteBuffer.wrap(value).getLong();
            return new Date(time);
        }

        @Override
        protected byte[] javaToByteArray(Date value) {
            ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE / 8);
            buffer.putLong(value.getTime());
            return buffer.array();
        }
    }

    public static class ObjectField<T> extends Field<T> {
        public ObjectField(java.lang.reflect.Field field, String columnFamily, String columnName) {
            super(field, columnFamily, columnName);
        }

        @Override
        protected T valueToJava(byte[] value) {
            try {
                ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(value));
                //noinspection unchecked
                return (T) in.readObject();
            } catch (Exception e) {
                throw new RuntimeException("Could not read map", e);
            }
        }

        @Override
        protected byte[] javaToByteArray(T value) {
            try {
                ByteArrayOutputStream buffer = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(buffer);
                out.writeObject(value);
                out.close();
                return buffer.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException("Could not write map", e);
            }
        }
    }

    public static class JSONObjectField extends Field<JSONObject> {
        public JSONObjectField(java.lang.reflect.Field field, String columnFamily, String columnName) {
            super(field, columnFamily, columnName);
        }

        @Override
        protected JSONObject valueToJava(byte[] value) {
            return new JSONObject(new String(value));
        }

        @Override
        protected byte[] javaToByteArray(JSONObject value) {
            return value.toString().getBytes();
        }
    }

    public static class IntegerField extends Field<Integer> {
        public IntegerField(java.lang.reflect.Field field, String columnFamily, String columnName) {
            super(field, columnFamily, columnName);
        }

        @Override
        protected Integer valueToJava(byte[] value) {
            return ByteBuffer.wrap(value).getInt();
        }

        @Override
        protected byte[] javaToByteArray(Integer value) {
            ByteBuffer buffer = ByteBuffer.allocate(Integer.SIZE / 8);
            buffer.putInt(value);
            return buffer.array();
        }
    }

    public static class LongField extends Field<Long> {
        public LongField(java.lang.reflect.Field field, String columnFamily, String columnName) {
            super(field, columnFamily, columnName);
        }

        @Override
        protected Long valueToJava(byte[] value) {
            return ByteBuffer.wrap(value).getLong();
        }

        @Override
        protected byte[] javaToByteArray(Long value) {
            ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE / 8);
            buffer.putLong(value);
            return buffer.array();
        }
    }

    public static class BooleanField extends Field<Boolean> {
        public BooleanField(java.lang.reflect.Field field, String columnFamily, String columnName) {
            super(field, columnFamily, columnName);
        }

        @Override
        protected Boolean valueToJava(byte[] value) {
            return value[0] != 0;
        }

        @Override
        protected byte[] javaToByteArray(Boolean value) {
            byte b = (byte) (value ? 1 : 0);
            return new byte[]{b};
        }
    }

    public static class ByteArrayField extends Field<byte[]> {
        public ByteArrayField(java.lang.reflect.Field field, String columnFamily, String columnName) {
            super(field, columnFamily, columnName);
        }

        @Override
        protected byte[] valueToJava(byte[] value) {
            return value;
        }

        @Override
        protected byte[] javaToByteArray(byte[] value) {
            return value;
        }
    }
}
