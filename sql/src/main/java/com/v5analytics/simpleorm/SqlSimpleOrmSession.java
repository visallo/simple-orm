package com.v5analytics.simpleorm;

import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.Date;

import static com.google.common.base.Preconditions.checkNotNull;

public class SqlSimpleOrmSession extends SimpleOrmSession {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlSimpleOrmSession.class);
    private static final int TABLE_NAME_COLUMN = 3;
    private static final String SQL_DROP_TABLE = "DROP TABLE %s";
    private static final String SQL_FIND_ALL = "SELECT * FROM %s";
    private static final String SQL_FIND_BY_ID = "SELECT * FROM %s WHERE id=?";
    private static final String SQL_FIND_BY_ID_STARTS_WITH = "SELECT * FROM %s WHERE id LIKE ?";
    private static final String SQL_ALTER_VISIBILITY = "UPDATE %s SET visibility=? WHERE id=?";
    private static final String SQL_DELETE = "DELETE FROM %s WHERE id=?";
    public static final String CONFIG_DRIVER_CLASS = "simpleOrm.sql.driverClass";
    public static final String CONFIG_CONNECTION_STRING = "simpleOrm.sql.connectionString";
    public static final String CONFIG_USER_NAME = "simpleOrm.sql.userName";
    public static final String CONFIG_PASSWORD = "simpleOrm.sql.password";
    private String jdbcConnectionString;
    private String jdbcUserName;
    private String jdbcPassword;
    private String tablePrefix;

    public void init(Map<String, Object> properties) {
        String jdbcDriverClass = (String) properties.get(CONFIG_DRIVER_CLASS);
        checkNotNull(jdbcDriverClass, "Missing configuration: " + CONFIG_DRIVER_CLASS);
        try {
            Class.forName(jdbcDriverClass);
        } catch (ClassNotFoundException e) {
            throw new SimpleOrmException("Could not find driver class: " + jdbcDriverClass, e);
        }
        jdbcConnectionString = (String) properties.get(CONFIG_CONNECTION_STRING);
        checkNotNull(jdbcConnectionString, "Missing configuration: " + CONFIG_CONNECTION_STRING);
        jdbcUserName = (String) properties.get(CONFIG_USER_NAME);
        jdbcPassword = (String) properties.get(CONFIG_PASSWORD);
        setTablePrefix(properties);
    }

    private void setTablePrefix(Map<String, Object> properties) {
        tablePrefix = (String) properties.get(TABLE_PREFIX);
        if (tablePrefix == null) {
            tablePrefix = "";
        }
    }

    @Override
    public SimpleOrmContext createContext(String... authorizations) {
        return new SqlSimpleOrmContext(authorizations);
    }

    @Override
    public String getTablePrefix() {
        return tablePrefix;
    }

    @Override
    public Iterable<String> getTableList(SimpleOrmContext context) {
        List<String> results = new ArrayList<>();
        try (Connection conn = getConnection(context)) {
            ResultSet rs = conn.getMetaData().getTables(null, null, "%", null);
            while (rs.next()) {
                String tableName = rs.getString(TABLE_NAME_COLUMN);
                results.add(tableName);
            }
        } catch (SQLException e) {
            throw new SimpleOrmException("Failed to get table names", e);
        }
        return results;
    }

    @Override
    public void deleteTable(String tableName, SimpleOrmContext context) {
        try (Connection conn = getConnection(context)) {
            String sql = String.format(SQL_DROP_TABLE, tableName);
            LOGGER.debug("sql: " + sql);
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new SimpleOrmException("Failed to delete table", e);
        }
    }

    @Override
    public <T> Iterable<T> findAll(Class<T> rowClass, SimpleOrmContext context) {
        ModelMetadata<T> modelMetadata = ModelMetadata.getModelMetadata(rowClass);
        try {
            Connection conn = getConnection(context);
            String sql = String.format(SQL_FIND_ALL, getTableName(modelMetadata));
            LOGGER.debug("sql: " + sql);
            PreparedStatement stmt = conn.prepareStatement(sql);
            return resultSetToRows(modelMetadata, conn, stmt.executeQuery());
        } catch (SQLException e) {
            throw handleSQLException(modelMetadata, "Failed to find all", e);
        }
    }

    @Override
    public <T> T findById(Class<T> rowClass, String id, SimpleOrmContext context) {
        ModelMetadata<T> modelMetadata = ModelMetadata.getModelMetadata(rowClass);
        try (Connection conn = getConnection(context)) {
            String sql = String.format(SQL_FIND_BY_ID, getTableName(modelMetadata));
            LOGGER.debug("sql: " + sql);
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setString(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                try (ClosableIterator<T> results = resultSetToRows(modelMetadata, conn, rs).iterator()) {
                    if (!results.hasNext()) {
                        return null;
                    }
                    T result = results.next();
                    if (results.hasNext()) {
                        throw new SimpleOrmException("Too many rows for the id: " + id);
                    }
                    return result;
                }
            }
        } catch (Exception e) {
            throw handleSQLException(modelMetadata, "Failed to find by id: " + id, e);
        }
    }

    @Override
    public <T> Iterable<T> findByIdStartsWith(Class<T> rowClass, String idPrefix, SimpleOrmContext context) {
        ModelMetadata<T> modelMetadata = ModelMetadata.getModelMetadata(rowClass);
        try {
            Connection conn = getConnection(context);
            String sql = String.format(SQL_FIND_BY_ID_STARTS_WITH, getTableName(modelMetadata));
            LOGGER.debug("sql: " + sql);
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setString(1, idPrefix + "%");
            return resultSetToRows(modelMetadata, conn, stmt.executeQuery());
        } catch (SQLException e) {
            throw handleSQLException(modelMetadata, "Failed to find by id starts with: " + idPrefix, e);
        }
    }

    private RuntimeException handleSQLException(ModelMetadata modelMetadata, String message, Exception e) {
        LOGGER.error(message, e);
        try {
            printCreateTable(modelMetadata);
        } catch (Throwable ex) {
            LOGGER.error("failed to print create table", ex);
        }
        return new SimpleOrmException(message, e);
    }

    private void printCreateTable(ModelMetadata modelMetadata) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(getTableName(modelMetadata)).append(" (\n");
        sb.append("  id VARCHAR(8000) PRIMARY KEY,\n");
        sb.append("  visibility VARCHAR(8000) NOT NULL,\n");
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
        LOGGER.debug("Did you create your table:\n " + sb.toString());
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
            return "VARCHAR(255)";
        }
        if (field instanceof ModelMetadata.JSONObjectField) {
            return "TEXT";
        }
        if (field instanceof ModelMetadata.ObjectField) {
            return "BYTEA";
        }
        if (field instanceof ModelMetadata.BooleanField) {
            return "BOOLEAN";
        }
        throw new SimpleOrmException("Could not get sql field type of: " + field.getClass().getName());
    }

    @Override
    public <T> void save(T obj, String visibility, SimpleOrmContext context) {
        ModelMetadata<T> modelMetadata = ModelMetadata.getModelMetadata(obj);
        ModelMetadata.Type modelMetadataType = modelMetadata.getTypeFromObject(obj);
        Collection<ModelMetadata.Field> allFields = modelMetadataType.getAllFields();
        String objId = modelMetadata.getId(obj);
        String sql;
        boolean isInsert;
        //noinspection unchecked
        T existingObj = (T) findById(obj.getClass(), objId, context);
        if (existingObj != null) {
            isInsert = false;
            sql = getUpdateSql(allFields, getTableName(modelMetadata));
        } else {
            isInsert = true;
            sql = getInsertSql(allFields, getTableName(modelMetadata));
        }
        try (Connection conn = getConnection(context)) {
            LOGGER.debug("sql: " + sql);
            PreparedStatement stmt = conn.prepareStatement(sql);
            int i = 1;
            if (isInsert) {
                stmt.setString(i++, objId);
            }
            stmt.setString(i++, visibility);
            for (ModelMetadata.Field field : allFields) {
                if (field instanceof ModelMetadata.StringField) {
                    stmt.setString(i++, ((ModelMetadata.StringField) field).getRaw(obj));
                } else if (field instanceof ModelMetadata.JSONObjectField) {
                    JSONObject raw = ((ModelMetadata.JSONObjectField) field).getRaw(obj);
                    stmt.setString(i++, raw == null ? null : raw.toString());
                } else if (field instanceof ModelMetadata.EnumField) {
                    Enum raw = ((ModelMetadata.EnumField) field).getRaw(obj);
                    stmt.setString(i++, raw == null ? null : raw.name());
                } else if (field instanceof ModelMetadata.IntegerField) {
                    stmt.setLong(i++, ((ModelMetadata.IntegerField) field).getRaw(obj));
                } else if (field instanceof ModelMetadata.LongField) {
                    stmt.setLong(i++, ((ModelMetadata.LongField) field).getRaw(obj));
                } else if (field instanceof ModelMetadata.DateField) {
                    Date raw = ((ModelMetadata.DateField) field).getRaw(obj);
                    stmt.setDate(i++, raw == null ? null : new java.sql.Date(raw.getTime()));
                } else if (field instanceof ModelMetadata.ObjectField || field instanceof ModelMetadata.ByteArrayField) {
                    byte[] raw = field.get(obj);
                    InputStream blobData = new ByteArrayInputStream(raw);
                    stmt.setBinaryStream(i++, blobData, raw.length);
                } else {
                    throw new SimpleOrmException("Could not store field: " + field.getClass().getName());
                }
            }
            if (!isInsert) {
                stmt.setString(i, objId);
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw handleSQLException(modelMetadata, "Failed to insert: " + obj, e);
        }
    }

    private String getUpdateSql(Collection<ModelMetadata.Field> allFields, String tableName) {
        StringBuilder result = new StringBuilder();
        result.append("UPDATE ").append(tableName).append(" SET visibility=?");
        for (ModelMetadata.Field field : allFields) {
            result.append(",").append(getColumnName(field)).append("=?");
        }
        result.append(" WHERE id=?");
        return result.toString();
    }

    private String getInsertSql(Collection<ModelMetadata.Field> allFields, String tableName) {
        StringBuilder result = new StringBuilder();
        result.append("INSERT INTO ").append(tableName).append(" (id,visibility");
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

    @Override
    public <T> void delete(Class<T> rowClass, String id, SimpleOrmContext context) {
        ModelMetadata<T> modelMetadata = ModelMetadata.getModelMetadata(rowClass);
        try (Connection conn = getConnection(context)) {
            String sql = String.format(SQL_DELETE, getTableName(modelMetadata));
            LOGGER.debug("sql: " + sql);
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setString(1, id);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw handleSQLException(modelMetadata, "Failed to delete: " + id, e);
        }
    }

    @Override
    public <T> void alterVisibility(T obj, String currentVisibility, String newVisibility, SimpleOrmContext context) {
        ModelMetadata<T> modelMetadata = ModelMetadata.getModelMetadata(obj);
        String objId = modelMetadata.getId(obj);
        try (Connection conn = getConnection(context)) {
            String sql = String.format(SQL_ALTER_VISIBILITY, getTableName(modelMetadata));
            LOGGER.debug("sql: " + sql);
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setString(1, newVisibility);
            stmt.setString(2, objId);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw handleSQLException(modelMetadata, "Failed to update visibility of: " + objId, e);
        }
    }

    @Override
    public void close() {

    }

    private <T> String getTableName(ModelMetadata<T> modelMetadata) {
        return tablePrefix + modelMetadata.getTableName();
    }

    public Connection getConnection(SimpleOrmContext context) throws SQLException {
        return DriverManager.getConnection(getJdbcConnectionString(context), getJdbcConnectionProperties(context));
    }

    private void closeConnection(Connection conn) throws SQLException {
        conn.close();
    }

    protected String getJdbcConnectionString(
            @SuppressWarnings("UnusedParameters") SimpleOrmContext context
    ) {
        return jdbcConnectionString;
    }

    protected Properties getJdbcConnectionProperties(
            @SuppressWarnings("UnusedParameters") SimpleOrmContext context
    ) {
        Properties properties = new Properties();
        properties.put("user", jdbcUserName);
        properties.put("password", jdbcPassword);
        return properties;
    }

    private <T> ClosableIterable<T> resultSetToRows(final ModelMetadata<T> modelMetadata, final Connection conn, final ResultSet resultSet) throws SQLException {
        final ResultSetMetaData resultSetMetadata = resultSet.getMetaData();
        final String discriminatorColumnName;
        if (modelMetadata.getDiscriminatorColumnFamily() != null || modelMetadata.getDiscriminatorColumnName() != null) {
            discriminatorColumnName = getColumnName(modelMetadata.getDiscriminatorColumnFamily(), modelMetadata.getDiscriminatorColumnName());
        } else {
            discriminatorColumnName = null;
        }
        final ModelMetadata.Type defaultType = modelMetadata.getType(null);
        return new ClosableIterable<T>() {
            @Override
            public ClosableIterator<T> iterator() {
                return new ClosableIterator<T>() {
                    private T next;

                    @Override
                    public boolean hasNext() {
                        try {
                            fetchNext();
                        } catch (Exception e) {
                            throw new SimpleOrmException("Could not fetch next", e);
                        }
                        return next != null;
                    }

                    @Override
                    public T next() {
                        T result = next;
                        next = null;
                        return result;
                    }

                    public void close() {
                        try {
                            if (!resultSet.isClosed()) {
                                resultSet.close();
                            }
                            if (conn != null && !conn.isClosed()) {
                                closeConnection(conn);
                            }
                        } catch (Exception ex) {
                            throw new SimpleOrmException("Could not close iterable", ex);
                        }
                    }

                    private void fetchNext() throws SQLException, IOException {
                        if (next != null || resultSet.isClosed()) {
                            return;
                        }
                        if (!resultSet.next()) {
                            close();
                            return;
                        }
                        ModelMetadata.Type type;
                        if (discriminatorColumnName != null) {
                            String discriminatorValue = resultSet.getString(discriminatorColumnName);
                            type = modelMetadata.getType(discriminatorValue);
                        } else {
                            type = defaultType;
                        }
                        Collection<ModelMetadata.Field> fields = type.getAllFields();
                        T result = type.newInstance();
                        modelMetadata.setIdField(result, resultSet.getString("id"));
                        for (int i = 1; i <= resultSetMetadata.getColumnCount(); i++) {
                            String columnLabel = resultSetMetadata.getColumnLabel(i);
                            ModelMetadata.Field field = findFieldByColumnName(fields, columnLabel);
                            try {
                                if (field != null) {
                                    if (field instanceof ModelMetadata.StringField) {
                                        field.setRaw(result, resultSet.getString(i));
                                    } else if (field instanceof ModelMetadata.EnumField) {
                                        String str = resultSet.getString(i);
                                        field.set(result, str == null ? null : str.getBytes());
                                    } else if (field instanceof ModelMetadata.LongField) {
                                        field.setRaw(result, resultSet.getLong(i));
                                    } else if (field instanceof ModelMetadata.IntegerField) {
                                        field.setRaw(result, resultSet.getInt(i));
                                    } else if (field instanceof ModelMetadata.BooleanField) {
                                        field.setRaw(result, resultSet.getBoolean(i));
                                    } else if (field instanceof ModelMetadata.DateField) {
                                        field.setRaw(result, resultSet.getDate(i));
                                    } else if (field instanceof ModelMetadata.JSONObjectField) {
                                        field.setRaw(result, new JSONObject(resultSet.getString(i)));
                                    } else if (field instanceof ModelMetadata.ObjectField || field instanceof ModelMetadata.ByteArrayField) {
                                        byte[] raw = IOUtils.toByteArray(resultSet.getBinaryStream(i));
                                        field.set(result, raw);
                                    } else {
                                        throw new SimpleOrmException("Could not populate field of type: " + field.getClass());
                                    }
                                }
                            } catch (Exception ex) {
                                throw new SimpleOrmException("Could not read sql column: " + columnLabel + " into field: " + field, ex);
                            }
                        }
                        next = result;
                    }

                    private ModelMetadata.Field findFieldByColumnName(Collection<ModelMetadata.Field> fields, String columnLabel) {
                        for (ModelMetadata.Field field : fields) {
                            if (getColumnName(field).equalsIgnoreCase(columnLabel)) {
                                return field;
                            }
                        }
                        return null;
                    }

                    @Override
                    public void remove() {
                        throw new SimpleOrmException("Not supported");
                    }
                };
            }
        };
    }

    private String getColumnName(ModelMetadata.Field field) {
        if (field instanceof ModelMetadata.IdField) {
            return "id";
        }
        String columnFamily = field.getColumnFamily();
        String columnName = field.getColumnName();
        return getColumnName(columnFamily, columnName);
    }

    private String getColumnName(String columnFamily, String columnName) {
        StringBuilder result = new StringBuilder();
        if (columnFamily != null && columnFamily.length() > 0) {
            result.append(columnFamily).append('_');
        }
        if (columnName != null && columnName.length() > 0) {
            result.append(columnName);
        }
        return result.toString();
    }

    private static interface ClosableIterable<T> extends Iterable<T> {
        ClosableIterator<T> iterator();
    }

    private static interface ClosableIterator<T> extends Iterator<T>, AutoCloseable {
    }
}
