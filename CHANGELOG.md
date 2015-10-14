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
