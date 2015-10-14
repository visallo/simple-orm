package com.v5analytics.simpleorm;

import org.json.JSONObject;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public abstract class TestBase {
    protected abstract SimpleOrmSession createSession();

    public void before() throws Exception {

    }

    public void after() throws Exception {

    }

    @Test
    public void testFindAll() {
        SimpleOrmSession session = createSession();
        SimpleOrmContext ctx = session.createContext();

        List<SimpleModelObject> items = newArrayList(session.findAll(SimpleModelObject.class, ctx));
        assertEquals("expected no items", 0, items.size());

        SimpleModelObject obj1 = new SimpleModelObject();
        obj1.setId("1");
        obj1.setIntColumn(42);
        obj1.setStringColumn("Hello World");
        obj1.setJsonColumn(new JSONObject("{ name: \"The Name\" }"));
        session.save(obj1, "", ctx);

        SimpleModelObject obj2 = new SimpleModelObject();
        obj2.setId("2");
        obj2.setIntColumn(100);
        obj2.setStringColumn("Simple ORM");
        obj2.setJsonColumn(new JSONObject("{ value: 12 }"));
        session.save(obj2, "", ctx);

        items = newArrayList(session.findAll(SimpleModelObject.class, ctx));
        assertItems(new SimpleModelObject[]{obj1, obj2}, items);

        session.close();
    }

    @Test
    public void testFindById() {
        SimpleOrmSession session = createSession();
        SimpleOrmContext ctx = session.createContext();

        SimpleModelObject obj1 = new SimpleModelObject();
        obj1.setId("1");
        obj1.setIntColumn(42);
        obj1.setStringColumn("Hello World");
        obj1.setJsonColumn(new JSONObject("{ name: \"The Name\" }"));
        session.save(obj1, "", ctx);

        SimpleModelObject obj2 = new SimpleModelObject();
        obj2.setId("2");
        obj2.setIntColumn(100);
        obj2.setStringColumn("Simple ORM");
        obj2.setJsonColumn(new JSONObject("{ value: 12 }"));
        session.save(obj2, "", ctx);

        SimpleModelObject item = session.findById(SimpleModelObject.class, "1", ctx);
        assertEquals(obj1, item);

        item = session.findById(SimpleModelObject.class, "2", ctx);
        assertEquals(obj2, item);

        item = session.findById(SimpleModelObject.class, "3", ctx);
        assertNull("Should not find item 3", item);

        session.close();
    }

    @Test
    public void testFindByIdStartsWith() {
        SimpleOrmSession session = createSession();
        SimpleOrmContext ctx = session.createContext();

        SimpleModelObject obj1 = new SimpleModelObject();
        obj1.setId("a1");
        obj1.setIntColumn(42);
        obj1.setStringColumn("Hello World");
        obj1.setJsonColumn(new JSONObject("{ name: \"The Name\" }"));
        session.save(obj1, "", ctx);

        SimpleModelObject obj2 = new SimpleModelObject();
        obj2.setId("a2");
        obj2.setIntColumn(100);
        obj2.setStringColumn("Simple ORM");
        obj2.setJsonColumn(new JSONObject("{ value: 12 }"));
        session.save(obj2, "", ctx);

        SimpleModelObject obj3 = new SimpleModelObject();
        obj3.setId("b1");
        obj3.setIntColumn(99);
        obj3.setStringColumn("Other object");
        obj3.setJsonColumn(new JSONObject("{ }"));
        session.save(obj3, "", ctx);

        List<SimpleModelObject> items = newArrayList(session.findByIdStartsWith(SimpleModelObject.class, "a", ctx));
        assertItems(new SimpleModelObject[]{obj1, obj2}, items);

        items = newArrayList(session.findByIdStartsWith(SimpleModelObject.class, "z", ctx));
        assertItems(new SimpleModelObject[]{}, items);

        session.close();
    }

    @Test
    public void testDelete() {
        SimpleOrmSession session = createSession();
        SimpleOrmContext ctx = session.createContext();

        List<SimpleModelObject> items = newArrayList(session.findAll(SimpleModelObject.class, ctx));
        assertEquals("expected no items", 0, items.size());

        SimpleModelObject obj1 = new SimpleModelObject();
        obj1.setId("1");
        obj1.setIntColumn(42);
        obj1.setStringColumn("Hello World");
        obj1.setJsonColumn(new JSONObject("{ name: \"The Name\" }"));
        session.save(obj1, "", ctx);

        SimpleModelObject obj2 = new SimpleModelObject();
        obj2.setId("2");
        obj2.setIntColumn(100);
        obj2.setStringColumn("Simple ORM");
        obj2.setJsonColumn(new JSONObject("{ value: 12 }"));
        session.save(obj2, "", ctx);

        items = newArrayList(session.findAll(SimpleModelObject.class, ctx));
        assertItems(new SimpleModelObject[]{obj1, obj2}, items);

        session.delete(SimpleModelObject.class, "1", ctx);

        items = newArrayList(session.findAll(SimpleModelObject.class, ctx));
        assertItems(new SimpleModelObject[]{obj2}, items);

        session.delete(SimpleModelObject.class, "3", ctx);

        items = newArrayList(session.findAll(SimpleModelObject.class, ctx));
        assertItems(new SimpleModelObject[]{obj2}, items);

        session.delete(SimpleModelObject.class, "2", ctx);

        items = newArrayList(session.findAll(SimpleModelObject.class, ctx));
        assertItems(new SimpleModelObject[]{}, items);

        session.close();
    }

    private <T extends Comparable<T>> void assertItems(T[] expected, List<T> found) {
        assertEquals(expected.length, found.size());
        Collections.sort(found);
        for (int i = 0; i < expected.length; i++) {
            assertEquals("Failed at index " + i, expected[i], found.get(i));
        }
    }
}
