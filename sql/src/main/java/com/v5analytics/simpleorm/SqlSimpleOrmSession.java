package com.v5analytics.simpleorm;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.*;
import java.util.Date;

public class SqlSimpleOrmSession extends SimpleOrmSession {
    private static final int TABLE_NAME_COLUMN = 3;
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlSimpleOrmSession.class);
    private static final String CONFIG_PREFIX = "simpleOrm.sql.";
    private final Set<String> existingTables = new HashSet<>();
    private SqlGenerator sqlGenerator;
    private DataSource dataSource;

    public void init(Map<String, Object> properties) {
        dataSource = createDataSource(properties);
        sqlGenerator = new SqlGenerator(getTablePrefix(properties));
    }

    private DataSource createDataSource(Map<String, Object> config) {
        Properties properties = new Properties();
        for (Map.Entry<String, Object> configEntry : config.entrySet()) {
            String key = configEntry.getKey();
            if (key.startsWith(CONFIG_PREFIX)) {
                key = key.substring(CONFIG_PREFIX.length());
                properties.put(key, configEntry.getValue());
            }
        }
        HikariConfig hikariConfig = new HikariConfig(properties);
        return new HikariDataSource(hikariConfig);
    }

    private static String getTablePrefix(Map<String, Object> properties) {
        String tablePrefix = (String) properties.get(TABLE_PREFIX);
        if (tablePrefix == null) {
            tablePrefix = "";
        }
        return tablePrefix;
    }

    @Override
    public SimpleOrmContext createContext(String... authorizations) {
        return new SqlSimpleOrmContext(authorizations);
    }

    @Override
    public String getTablePrefix() {
        return sqlGenerator.getTablePrefix();
    }

    @Override
    public Set<String> getTableList(SimpleOrmContext context) {
        Set<String> results = new HashSet<>();
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
            String sql = sqlGenerator.getDropTableSql(tableName);
            LOGGER.debug("sql: " + sql);
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new SimpleOrmException("Failed to delete table", e);
        }
    }

    @Override
    public void clearTable(String table, SimpleOrmContext context) {
        try (Connection conn = getConnection(context)) {
            String sql = sqlGenerator.getClearTableSql(table);
            LOGGER.debug("sql: " + sql);
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new SimpleOrmException("Failed to clear table", e);
        }
    }

    @Override
    public <T> Iterable<T> findAll(Class<T> rowClass, SimpleOrmContext context) {
        ModelMetadata<T> modelMetadata = getModelMetadata(context, rowClass);
        try {
            Connection conn = getConnection(context);
            String sql = sqlGenerator.getFindAllSql(modelMetadata);
            LOGGER.debug("sql: " + sql);
            PreparedStatement stmt = conn.prepareStatement(sql);
            return resultSetToRows(modelMetadata, conn, stmt.executeQuery());
        } catch (SQLException e) {
            throw new SimpleOrmException("Failed to find all", e);
        }
    }

    @Override
    public <T> T findById(Class<T> rowClass, String id, SimpleOrmContext context) {
        ModelMetadata<T> modelMetadata = getModelMetadata(context, rowClass);
        try (Connection conn = getConnection(context)) {
            String sql = sqlGenerator.getFindByIdSql(modelMetadata);
            LOGGER.debug("sql: " + sql);
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setString(1, id);
            try (ResultSet rs = stmt.executeQuery()) {
                try (CloseableIterator<T> results = resultSetToRows(modelMetadata, conn, rs).iterator()) {
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
            throw new SimpleOrmException("Failed to find by id: " + id, e);
        }
    }

    @Override
    public <T> Iterable<T> findByIdStartsWith(Class<T> rowClass, String idPrefix, SimpleOrmContext context) {
        ModelMetadata<T> modelMetadata = getModelMetadata(context, rowClass);
        try {
            Connection conn = getConnection(context);
            String sql = sqlGenerator.getFindByIdStartsWithSql(modelMetadata);
            LOGGER.debug("sql: " + sql);
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setString(1, idPrefix + "%");
            return resultSetToRows(modelMetadata, conn, stmt.executeQuery());
        } catch (SQLException e) {
            throw new SimpleOrmException("Failed to find by id starts with: " + idPrefix, e);
        }
    }

    @Override
    public <T> void save(T obj, String visibility, SimpleOrmContext context) {
        ModelMetadata<T> modelMetadata = getModelMetadata(context, obj);
        ModelMetadata.Type modelMetadataType = modelMetadata.getTypeFromObject(obj);
        Collection<ModelMetadata.Field> allFields = modelMetadataType.getAllFields();
        String objId = modelMetadata.getId(obj);
        String sql;
        boolean isInsert;
        //noinspection unchecked
        T existingObj = (T) findById(obj.getClass(), objId, context);
        if (existingObj != null) {
            isInsert = false;
            sql = sqlGenerator.getUpdateSql(modelMetadata, allFields);
        } else {
            isInsert = true;
            sql = sqlGenerator.getInsertSql(modelMetadata, allFields);
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
                    stmt.setString(i, ((ModelMetadata.StringField) field).getRaw(obj));
                } else if (field instanceof ModelMetadata.JSONObjectField) {
                    JSONObject raw = ((ModelMetadata.JSONObjectField) field).getRaw(obj);
                    stmt.setString(i, raw == null ? null : raw.toString());
                } else if (field instanceof ModelMetadata.EnumField) {
                    Enum raw = ((ModelMetadata.EnumField) field).getRaw(obj);
                    stmt.setString(i, raw == null ? null : raw.name());
                } else if (field instanceof ModelMetadata.IntegerField) {
                    if (!setIfNullValue(stmt, i, field, Types.INTEGER, obj)) {
                        stmt.setInt(i, ((ModelMetadata.IntegerField) field).getRaw(obj));
                    }
                } else if (field instanceof ModelMetadata.BooleanField) {
                    if (!setIfNullValue(stmt, i, field, Types.BOOLEAN, obj)) {
                        stmt.setBoolean(i, ((ModelMetadata.BooleanField) field).getRaw(obj));
                    }
                } else if (field instanceof ModelMetadata.LongField) {
                    if (!setIfNullValue(stmt, i, field, Types.INTEGER, obj)) {
                        stmt.setLong(i, ((ModelMetadata.LongField) field).getRaw(obj));
                    }
                } else if (field instanceof ModelMetadata.DateField) {
                    Date raw = ((ModelMetadata.DateField) field).getRaw(obj);
                    stmt.setTimestamp(i, raw == null ? null : new Timestamp(raw.getTime()));
                } else if (field instanceof ModelMetadata.ObjectField || field instanceof ModelMetadata.ByteArrayField) {
                    byte[] raw = field.get(obj);
                    if (raw == null) {
                        stmt.setBlob(i, (InputStream) null);
                    } else {
                        InputStream blobData = new ByteArrayInputStream(raw);
                        stmt.setBinaryStream(i, blobData, raw.length);
                    }
                } else {
                    throw new SimpleOrmException("Could not store field: " + field.getClass().getName());
                }
                i += 1;
            }
            if (!isInsert) {
                stmt.setString(i, objId);
            }
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new SimpleOrmException("Failed to insert: " + obj, e);
        }
    }

    /**
     * Use this method to set null on the statement for auto-boxed field values that could be null.
     *
     * @return true if the field value is null and the statement was set null; false if the field value is non-null.
     */
    private boolean setIfNullValue(PreparedStatement stmt, int paramIndex, ModelMetadata.Field field, int sqlType,
                                   Object obj) throws SQLException {
        Object raw = field.getRaw(obj);
        if (raw == null) {
            stmt.setNull(paramIndex, sqlType, null);
            return true;
        } else {
            return false;
        }
    }

    @Override
    public <T> void delete(Class<T> rowClass, String id, SimpleOrmContext context) {
        ModelMetadata<T> modelMetadata = getModelMetadata(context, rowClass);
        try (Connection conn = getConnection(context)) {
            String sql = sqlGenerator.getDeleteByIdSql(modelMetadata);
            LOGGER.debug("sql: " + sql);
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setString(1, id);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new SimpleOrmException("Failed to delete: " + id, e);
        }
    }

    @Override
    public <T> void alterVisibility(T obj, String currentVisibility, String newVisibility, SimpleOrmContext context) {
        ModelMetadata<T> modelMetadata = getModelMetadata(context, obj);
        String objId = modelMetadata.getId(obj);
        try (Connection conn = getConnection(context)) {
            String sql = sqlGenerator.getAlterVisibilitySql(modelMetadata);
            LOGGER.debug("sql: " + sql);
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.setString(1, newVisibility);
            stmt.setString(2, objId);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new SimpleOrmException("Failed to update visibility of: " + objId, e);
        }
    }

    @Override
    public void close() {

    }

    public Connection getConnection(SimpleOrmContext context) throws SQLException {
        return dataSource.getConnection();
    }

    private void closeConnection(Connection conn) throws SQLException {
        conn.close();
    }

    private <T> CloseableIterable<T> resultSetToRows(final ModelMetadata<T> modelMetadata, final Connection conn, final ResultSet resultSet) throws SQLException {
        final ResultSetMetaData resultSetMetadata = resultSet.getMetaData();
        final String discriminatorColumnName;
        if (modelMetadata.getDiscriminatorColumnFamily() != null || modelMetadata.getDiscriminatorColumnName() != null) {
            discriminatorColumnName = sqlGenerator.getColumnName(modelMetadata.getDiscriminatorColumnFamily(), modelMetadata.getDiscriminatorColumnName());
        } else {
            discriminatorColumnName = null;
        }
        final ModelMetadata.Type defaultType = modelMetadata.getType(null);
        return new CloseableIterable<T>() {
            @Override
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

            @Override
            public CloseableIterator<T> iterator() {
                return new CloseableIterator<T>() {
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

                    @Override
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

                    @SuppressWarnings("unchecked")
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
                                        long rsLong = resultSet.getLong(i);
                                        boolean wasNull = resultSet.wasNull();
                                        field.setRaw(result, wasNull ? null : rsLong);
                                    } else if (field instanceof ModelMetadata.IntegerField) {
                                        int rsInt = resultSet.getInt(i);
                                        boolean wasNull = resultSet.wasNull();
                                        field.setRaw(result, wasNull ? null : rsInt);
                                    } else if (field instanceof ModelMetadata.BooleanField) {
                                        boolean rsBoolean = resultSet.getBoolean(i);
                                        boolean wasNull = resultSet.wasNull();
                                        field.setRaw(result, wasNull ? null : rsBoolean);
                                    } else if (field instanceof ModelMetadata.DateField) {
                                        Timestamp timestamp = resultSet.getTimestamp(i);
                                        field.setRaw(result, timestamp == null ? null : new Date(timestamp.getTime()));
                                    } else if (field instanceof ModelMetadata.JSONObjectField) {
                                        String str = resultSet.getString(i);
                                        field.setRaw(result, str == null ? null : new JSONObject(str));
                                    } else if (field instanceof ModelMetadata.ObjectField || field instanceof ModelMetadata.ByteArrayField) {
                                        InputStream value = resultSet.getBinaryStream(i);
                                        if (value == null) {
                                            field.set(result, null);
                                        } else {
                                            byte[] raw = IOUtils.toByteArray(value);
                                            field.set(result, raw);
                                        }
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
                            if (sqlGenerator.getColumnName(field).equalsIgnoreCase(columnLabel)) {
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

    private <T> ModelMetadata<T> getModelMetadata(SimpleOrmContext context, Class<T> rowClass) {
        ModelMetadata<T> modelMetadata = ModelMetadata.getModelMetadata(rowClass);
        createTableIfNotExists(context, modelMetadata);
        return modelMetadata;
    }

    private <T> ModelMetadata<T> getModelMetadata(SimpleOrmContext context, T rowObject) {
        ModelMetadata<T> modelMetadata = ModelMetadata.getModelMetadata(rowObject);
        createTableIfNotExists(context, modelMetadata);
        return modelMetadata;
    }

    private <T> void createTableIfNotExists(SimpleOrmContext context, ModelMetadata<T> modelMetadata) {
        if (existingTables.size() == 0) {
            existingTables.addAll(getTableList(context));
        }

        String tableName = sqlGenerator.getTableName(modelMetadata);
        if (!existingTables.contains(tableName)) {
            LOGGER.info("Table \"" + tableName + "\" not found. Creating...");
            try (Connection conn = getConnection(context)) {
                String sql = sqlGenerator.getCreateTableSql(tableName, modelMetadata);
                LOGGER.debug(sql);
                PreparedStatement stmt = conn.prepareStatement(sql);
                stmt.execute();

                existingTables.add(tableName);
            } catch (Exception ex) {
                throw new SimpleOrmException("Could not create table: " + tableName, ex);
            }
        }
    }
}
