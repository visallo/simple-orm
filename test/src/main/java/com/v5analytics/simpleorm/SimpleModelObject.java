package com.v5analytics.simpleorm;

import org.json.JSONObject;

import java.util.Arrays;
import java.util.Date;

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
    private boolean booleanColumn;

    @Field
    private Boolean nullableBooleanColumn;

    @Field
    private byte[] byteArrayColumn;

    @Field
    private Date dateColumn;

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

    public boolean isBooleanColumn() {
        return booleanColumn;
    }

    public void setBooleanColumn(boolean booleanColumn) {
        this.booleanColumn = booleanColumn;
    }

    public Boolean getNullableBooleanColumn() {
        return nullableBooleanColumn;
    }

    public void setNullableBooleanColumn(Boolean nullableBooleanColumn) {
        this.nullableBooleanColumn = nullableBooleanColumn;
    }

    public byte[] getByteArrayColumn() {
        return byteArrayColumn;
    }

    public void setByteArrayColumn(byte[] byteArrayColumn) {
        this.byteArrayColumn = byteArrayColumn;
    }

    public Date getDateColumn() {
        return dateColumn;
    }

    public void setDateColumn(Date dateColumn) {
        this.dateColumn = dateColumn;
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
    public String toString() {
        return "SimpleModelObject{" +
                "id='" + id + '\'' +
                ", stringColumn='" + stringColumn + '\'' +
                ", nullableStringColumn='" + nullableStringColumn + '\'' +
                ", intColumn=" + intColumn +
                ", nullableIntColumn=" + nullableIntColumn +
                ", booleanColumn=" + booleanColumn +
                ", nullableBooleanColumn=" + nullableBooleanColumn +
                ", byteArrayColumn=" + Arrays.toString(byteArrayColumn) +
                ", dateColumn=" + dateColumn +
                ", jsonColumn=" + jsonColumn +
                ", nullableJsonColumn=" + nullableJsonColumn +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SimpleModelObject that = (SimpleModelObject) o;

        if (intColumn != that.intColumn) return false;
        if (booleanColumn != that.booleanColumn) return false;
        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (stringColumn != null ? !stringColumn.equals(that.stringColumn) : that.stringColumn != null) return false;
        if (nullableStringColumn != null ? !nullableStringColumn.equals(that.nullableStringColumn) : that.nullableStringColumn != null)
            return false;
        if (nullableIntColumn != null ? !nullableIntColumn.equals(that.nullableIntColumn) : that.nullableIntColumn != null)
            return false;
        if (nullableBooleanColumn != null ? !nullableBooleanColumn.equals(that.nullableBooleanColumn) : that.nullableBooleanColumn != null)
            return false;
        if (!Arrays.equals(byteArrayColumn, that.byteArrayColumn)) return false;
        if (dateColumn != null ? !dateColumn.equals(that.dateColumn) : that.dateColumn != null) return false;
        if (jsonColumn != null ? !jsonColumn.toString().equals(that.jsonColumn.toString()) : that.jsonColumn != null)
            return false;
        return nullableJsonColumn != null ? nullableJsonColumn.equals(that.nullableJsonColumn) : that.nullableJsonColumn == null;
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }
}
