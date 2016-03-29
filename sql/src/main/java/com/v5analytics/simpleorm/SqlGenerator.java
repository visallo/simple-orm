package com.v5analytics.simpleorm;

import java.util.Collection;

public class SqlGenerator {
    public static final String SQL_DROP_TABLE = "DROP TABLE %s";
    public static final String SQL_CLEAR_TABLE = "DELETE FROM %s";
    public static final String SQL_FIND_ALL = "SELECT * FROM %s";
    public static final String SQL_FIND_BY_ID = "SELECT * FROM %s WHERE id=?";
    public static final String SQL_FIND_BY_ID_STARTS_WITH = "SELECT * FROM %s WHERE id LIKE ?";
    public static final String SQL_ALTER_VISIBILITY = "UPDATE %s SET visibility=? WHERE id=?";
    public static final String SQL_DELETE = "DELETE FROM %s WHERE id=?";

    private final String tablePrefix;

    // MySQL's limit is 767 (http://dev.mysql.com/doc/refman/5.7/en/innodb-restrictions.html)
    private static final int ID_VARCHAR_SIZE = 767;

    // Oracle's limit is 4000 (https://docs.oracle.com/cd/B28359_01/server.111/b28320/limits001.htm)
    // MySQL's limit is 65,535 (http://dev.mysql.com/doc/refman/5.7/en/char.html)
    // H2's limit is Integer.MAX_VALUE (http://www.h2database.com/html/datatypes.html#varchar_type)
    private static final int VARCHAR_SIZE = 4000;

    public SqlGenerator(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    public String getCreateTableSql(String tableName, ModelMetadata modelMetadata) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (\n");
        sb.append("  id VARCHAR(" + ID_VARCHAR_SIZE + ") PRIMARY KEY,\n");
        sb.append("  visibility VARCHAR(" + VARCHAR_SIZE + ") NOT NULL,\n");
        boolean first = true;
        for (Object oField : modelMetadata.getFields()) {
            if (!first) {
                sb.append(",\n");
            }
            ModelMetadata.Field field = (ModelMetadata.Field) oField;
            String columnName = getColumnName(field);
            String sqlType = getSqlType(field);
            sb.append("  ").append(columnName).append(" ").append(sqlType);
            first = false;
        }
        sb.append("\n);");
        return sb.toString();
    }

    private String getSqlType(ModelMetadata.Field field) {
        if (field instanceof ModelMetadata.StringField) {
            return "TEXT";
        }
        if (field instanceof ModelMetadata.LongField) {
            return "BIGINT";
        }
        if (field instanceof ModelMetadata.IntegerField) {
            return "INTEGER";
        }
        if (field instanceof ModelMetadata.DateField) {
            return "TIMESTAMP";
        }
        if (field instanceof ModelMetadata.EnumField) {
            return "VARCHAR(" + VARCHAR_SIZE + ")";
        }
        if (field instanceof ModelMetadata.JSONObjectField) {
            return "TEXT";
        }
        if (field instanceof ModelMetadata.ObjectField || field instanceof ModelMetadata.ByteArrayField) {
            return "LONGBLOB";
        }
        if (field instanceof ModelMetadata.BooleanField) {
            return "BOOLEAN";
        }
        throw new SimpleOrmException("Could not get sql field type of: " + field.getClass().getName());
    }

    public String getColumnName(ModelMetadata.Field field) {
        if (field instanceof ModelMetadata.IdField) {
            return "id";
        }
        String columnFamily = field.getColumnFamily();
        String columnName = field.getColumnName();
        return getColumnName(columnFamily, columnName);
    }

    public String getColumnName(String columnFamily, String columnName) {
        StringBuilder result = new StringBuilder();
        if (columnFamily != null && columnFamily.length() > 0) {
            result.append(columnFamily).append('_');
        }
        if (columnName != null && columnName.length() > 0) {
            result.append(columnName);
        }
        return result.toString();
    }

    public <T> String getTableName(ModelMetadata<T> modelMetadata) {
        return tablePrefix + modelMetadata.getTableName();
    }

    public String getTablePrefix() {
        return tablePrefix;
    }

    public String getDropTableSql(String tableName) {
        return String.format(SQL_DROP_TABLE, tableName);
    }

    public String getClearTableSql(String tableName) {
        return String.format(SQL_CLEAR_TABLE, tableName);
    }

    public <T> String getFindAllSql(ModelMetadata<T> modelMetadata) {
        return String.format(SQL_FIND_ALL, getTableName(modelMetadata));
    }

    public <T> String getFindByIdSql(ModelMetadata<T> modelMetadata) {
        return String.format(SQL_FIND_BY_ID, getTableName(modelMetadata));
    }

    public <T> String getFindByIdStartsWithSql(ModelMetadata<T> modelMetadata) {
        return String.format(SQL_FIND_BY_ID_STARTS_WITH, getTableName(modelMetadata));
    }

    public <T> String getUpdateSql(ModelMetadata<T> modelMetadata, Collection<ModelMetadata.Field> allFields) {
        StringBuilder result = new StringBuilder();
        result.append("UPDATE ").append(getTableName(modelMetadata)).append(" SET visibility=?");
        for (ModelMetadata.Field field : allFields) {
            result.append(",").append(getColumnName(field)).append("=?");
        }
        result.append(" WHERE id=?");
        return result.toString();
    }

    public <T> String getInsertSql(ModelMetadata<T> modelMetadata, Collection<ModelMetadata.Field> allFields) {
        StringBuilder result = new StringBuilder();
        result.append("INSERT INTO ").append(getTableName(modelMetadata)).append(" (id,visibility");
        for (ModelMetadata.Field field : allFields) {
            result.append(",").append(getColumnName(field));
        }
        result.append(") VALUES (?,?");
        //noinspection UnusedDeclaration
        for (ModelMetadata.Field field : allFields) {
            result.append(",?");
        }
        result.append(")");
        return result.toString();
    }

    public <T> String getDeleteByIdSql(ModelMetadata<T> modelMetadata) {
        return String.format(SQL_DELETE, getTableName(modelMetadata));
    }

    public <T> String getAlterVisibilitySql(ModelMetadata<T> modelMetadata) {
        return String.format(SQL_ALTER_VISIBILITY, getTableName(modelMetadata));
    }
}
