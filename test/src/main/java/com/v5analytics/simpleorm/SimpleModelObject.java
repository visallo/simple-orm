package com.v5analytics.simpleorm;

import org.json.JSONObject;

@Entity(tableName = "simpleModelObject")
public class SimpleModelObject implements Comparable<SimpleModelObject> {
    @Id
    private String id;

    @Field
    private String stringColumn;

    @Field
    private String nullableStringColumn;

    @Field
    private int intColumn;

    @Field
    private Integer nullableIntColumn;

    @Field
    private JSONObject jsonColumn;

    @Field
    private JSONObject nullableJsonColumn;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getStringColumn() {
        return stringColumn;
    }

    public void setStringColumn(String stringColumn) {
        this.stringColumn = stringColumn;
    }

    public String getNullableStringColumn() {
        return nullableStringColumn;
    }

    public void setNullableStringColumn(String nullableStringColumn) {
        this.nullableStringColumn = nullableStringColumn;
    }

    public int getIntColumn() {
        return intColumn;
    }

    public void setIntColumn(int intColumn) {
        this.intColumn = intColumn;
    }

    public Integer getNullableIntColumn() {
        return nullableIntColumn;
    }

    public void setNullableIntColumn(Integer nullableIntColumn) {
        this.nullableIntColumn = nullableIntColumn;
    }

    public JSONObject getJsonColumn() {
        return jsonColumn;
    }

    public void setJsonColumn(JSONObject jsonColumn) {
        this.jsonColumn = jsonColumn;
    }

    public JSONObject getNullableJsonColumn() {
        return nullableJsonColumn;
    }

    public void setNullableJsonColumn(JSONObject nullableJsonColumn) {
        this.nullableJsonColumn = nullableJsonColumn;
    }

    @Override
    public int compareTo(SimpleModelObject o) {
        return getId().compareTo(o.getId());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SimpleModelObject that = (SimpleModelObject) o;

        if (intColumn != that.intColumn) {
            return false;
        }
        if (id != null ? !id.equals(that.id) : that.id != null) {
            return false;
        }
        return !(stringColumn != null ? !stringColumn.equals(that.stringColumn) : that.stringColumn != null);

    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }
}
