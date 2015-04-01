package org.neolumin.simpleOrm;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class InMemoryTable<T> {
    private final ModelMetadata<T> modelMetadata;
    private final List<Item> data = new ArrayList<>();

    public InMemoryTable(Class<T> rowClass) {
        this.modelMetadata = ModelMetadataBuilder.build(rowClass);
    }

    public Class getEntityRowClass() {
        return this.modelMetadata.getEntityRowClass();
    }

    public String getName() {
        return this.modelMetadata.getTableName();
    }

    public Iterable<T> findAll(final SimpleOrmContext context) {
        final Iterable<Item> allItemsIterable = findAllItems(context);
        return new Iterable<T>() {
            @Override
            public Iterator<T> iterator() {
                final Iterator<Item> it = allItemsIterable.iterator();
                return new Iterator<T>() {
                    @Override
                    public boolean hasNext() {
                        return it.hasNext();
                    }

                    @Override
                    public T next() {
                        return it.next().getObj();
                    }

                    @Override
                    public void remove() {
                        it.remove();
                    }
                };
            }
        };
    }

    public Iterable<Item> findAllItems(SimpleOrmContext context) {
        // TODO filter by authorizations
        return this.data;
    }

    public T findById(String id, SimpleOrmContext context) {
        Item item = findItemById(id, context);
        if (item == null) {
            return null;
        }
        return item.getObj();
    }

    private Item findItemById(String id, SimpleOrmContext context) {
        for (Item item : findAllItems(context)) {
            if (this.modelMetadata.getId(item.getObj()).equals(id)) {
                return item;
            }
        }
        return null;
    }

    public Iterable<T> findByIdStartsWith(String idPrefix, SimpleOrmContext context) {
        List<T> results = new ArrayList<>();
        for (T t : findAll(context)) {
            if (this.modelMetadata.getId(t).startsWith(idPrefix)) {
                results.add(t);
            }
        }
        return results;
    }

    public void save(T obj, String visibility, SimpleOrmContext context) {
        String id = this.modelMetadata.getId(obj);
        delete(id, context);
        this.data.add(new Item(id, obj, visibility));
    }

    public void delete(String id, SimpleOrmContext context) {
        Item item = findItemById(id, context);
        if (item == null) {
            return;
        }
        this.data.remove(item);
    }

    public void alterVisibility(T obj, String currentVisibility, String newVisibility, SimpleOrmContext context) {
        String id = this.modelMetadata.getId(obj);
        Item item = findItemById(id, context);
        if (item == null) {
            return;
        }
        item.setVisibility(newVisibility);
    }

    private class Item {
        private final String id;
        private final T obj;
        private String visibility;

        public Item(String id, T obj, String visibility) {
            this.id = id;
            this.obj = obj;
            this.visibility = visibility;
        }

        public T getObj() {
            return obj;
        }

        public String getVisibility() {
            return visibility;
        }

        public void setVisibility(String visibility) {
            this.visibility = visibility;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            //noinspection unchecked
            Item item = (Item) o;

            return !(id != null ? !id.equals(item.id) : item.id != null);

        }

        @Override
        public int hashCode() {
            return id != null ? id.hashCode() : 0;
        }
    }
}
