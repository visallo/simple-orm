package com.v5analytics.simpleorm;

import java.util.Collection;

public abstract class SimpleOrmSession {
    public static final String TABLE_PREFIX = "simpleOrm.tablePrefix";

    public abstract SimpleOrmContext createContext(String... authorizations);

    public abstract Iterable<String> getTableList(SimpleOrmContext simpleOrmContext);

    public abstract void deleteTable(String table, SimpleOrmContext simpleOrmContext);

    public abstract <T> Iterable<T> findAll(Class<T> rowClass, SimpleOrmContext context);

    public abstract <T> T findById(Class<T> rowClass, String id, SimpleOrmContext context);

    public abstract <T> Iterable<T> findByIdStartsWith(Class<T> rowClass, String idPrefix, SimpleOrmContext simpleOrmContext);

    public abstract <T> void save(T obj, String visibility, SimpleOrmContext context);

    public <T> void saveMany(Collection<T> objs, String visibility, SimpleOrmContext context) {
        for (T obj : objs) {
            save(obj, visibility, context);
        }
    }

    public abstract <T> void delete(final Class<T> rowClass, String id, SimpleOrmContext context);

    public abstract <T> void alterVisibility(T obj, String currentVisibility, String newVisibility, SimpleOrmContext context);

    public abstract void close();
}
