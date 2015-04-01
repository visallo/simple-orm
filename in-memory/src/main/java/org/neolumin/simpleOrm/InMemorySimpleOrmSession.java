package org.neolumin.simpleOrm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InMemorySimpleOrmSession extends SimpleOrmSession {
    private Map<Class, InMemoryTable> tables = new HashMap<>();

    @Override
    public SimpleOrmContext createContext(String... authorizations) {
        return new InMemorySimpleOrmContext(authorizations);
    }

    @Override
    public Iterable<String> getTableList(SimpleOrmContext simpleOrmContext) {
        List<String> tableList = new ArrayList<>();
        for (InMemoryTable table : tables.values()) {
            tableList.add(table.getName());
        }
        return tableList;
    }

    @Override
    public void deleteTable(String tableName, SimpleOrmContext simpleOrmContext) {
        for (Map.Entry<Class, InMemoryTable> tableEntry : tables.entrySet()) {
            if (tableEntry.getValue().getName().equals(tableName)) {
                tables.remove(tableEntry.getKey());
            }
        }
    }

    @Override
    public <T> Iterable<T> findAll(Class<T> rowClass, SimpleOrmContext context) {
        InMemoryTable<T> table = getTable(rowClass);
        return table.findAll(context);
    }

    @Override
    public <T> T findById(Class<T> rowClass, String id, SimpleOrmContext context) {
        InMemoryTable<T> table = getTable(rowClass);
        return table.findById(id, context);
    }

    @Override
    public <T> Iterable<T> findByIdStartsWith(Class<T> rowClass, String idPrefix, SimpleOrmContext context) {
        InMemoryTable<T> table = getTable(rowClass);
        return table.findByIdStartsWith(idPrefix, context);
    }

    @Override
    public <T> void save(T obj, String visibility, SimpleOrmContext context) {
        //noinspection unchecked
        InMemoryTable<T> table = (InMemoryTable<T>) getTable(obj.getClass());
        table.save(obj, visibility, context);
    }

    @Override
    public <T> void delete(Class<T> rowClass, String id, SimpleOrmContext context) {
        InMemoryTable table = getTable(rowClass);
        table.delete(id, context);
    }

    @Override
    public <T> void alterVisibility(T obj, String currentVisibility, String newVisibility, SimpleOrmContext context) {
        InMemoryTable table = getTable(obj.getClass());
        table.alterVisibility(obj, currentVisibility, newVisibility, context);
    }

    protected <T> InMemoryTable<T> getTable(Class<T> rowClass) {
        //noinspection unchecked
        InMemoryTable<T> table = tables.get(rowClass);
        if (table == null) {
            Class c = rowClass.getSuperclass();
            while (c != null && table == null) {
                //noinspection unchecked
                table = tables.get(c);
                c = c.getSuperclass();
            }
        }
        if (table != null) {
            return table;
        }
        table = new InMemoryTable<>(rowClass);
        tables.put(table.getEntityRowClass(), table);
        return table;
    }

    @Override
    public void close() {
    }
}
