# v1.3.0

* Accumulo: Change version of Accumulo from 1.6.6 to 1.7.2
* Accumulo: use ClientConfiguration when connecting to accumulo to avoid warnings

# v1.2.0

* SQL: Use HikariCP for SQL connection pooling

# v1.1.1

* SQL: Change max primary key in DDL to 767 to fix MySQL

# v1.1.0

* SQL: Create table if the table does not already exist
* SQL: Fix date handling, now supports time
* SQL: Better support for null values

# v1.0.2

* Fixed more issues with null values

# v1.0.1

* Support for byte array fields
* Added method to clear table (delete all rows)
* Fixed bug where boolean fields weren't being saved
* Fixed bug where null Integer/Long/Boolean values weren't able to be saved

# v1.0.0

* Data store implementations:
  * Accumulo
  * SQL/JDBC
  * In-Memory
* Servlet session management implementations:
  * Jetty (`org.eclipse.jetty.server.SessionManager`)
  * Spring Framework (`org.springframework.session.SessionRepository`)
