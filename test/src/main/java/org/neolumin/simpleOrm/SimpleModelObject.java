package org.neolumin.simpleOrm;

@Entity(tableName = "simpleModelObject")
public class SimpleModelObject implements Comparable<SimpleModelObject> {
    @Id
    private String id;

    @Field
    private String stringColumn;

    @Field
    private int intColumn;

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

    public int getIntColumn() {
        return intColumn;
    }

    public void setIntColumn(int intColumn) {
        this.intColumn = intColumn;
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
