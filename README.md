# simple-orm [![Build Status](https://travis-ci.org/v5analytics/simple-orm.svg?branch=master)](https://travis-ci.org/v5analytics/simple-orm)

An ORM solution which talks with SQL and big data database solutions using the same code.

### Example Model Object

```java
@Entity(tableName = "user")
public class User {
    @Id
    private String id;

    @Field
    private String name;
    
    ... getters and setters ...
}
```

### Example Usage

```java
AccumuloSimpleOrmSession session = new AccumuloSimpleOrmSession();
SimpleOrmContext ctx = session.createContext();
session.findAll(User.class, ctx);
```
