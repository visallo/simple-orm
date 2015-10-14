package com.v5analytics.simpleorm;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertTrue;

public class SqlTest extends TestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlTest.class);
    private String connectionString;
    private File tempDir;

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        tempDir = File.createTempFile("accumulo-simple-orm-test-", Long.toString(System.nanoTime()));
        assertTrue(tempDir.delete());
        assertTrue(tempDir.mkdir());
        LOGGER.info("writing to: " + tempDir);
        connectionString = "jdbc:h2:" + tempDir.getAbsolutePath();
    }

    @After
    @Override
    public void after() throws Exception {
        super.after();
        assertTrue(tempDir.delete());
    }

    @Override
    protected SimpleOrmSession createSession() {
        SqlSimpleOrmSession session = new SqlSimpleOrmSession();
        Map<String, Object> properties = new HashMap<>();
        properties.put(SqlSimpleOrmSession.CONFIG_DRIVER_CLASS, "org.h2.Driver");
        properties.put(SqlSimpleOrmSession.CONFIG_CONNECTION_STRING, connectionString);
        properties.put(SqlSimpleOrmSession.CONFIG_USER_NAME, "sa");
        properties.put(SqlSimpleOrmSession.CONFIG_PASSWORD, "");
        session.init(properties);

        try (Connection conn = session.getConnection(session.createContext())) {
            String sql = "CREATE TABLE simpleModelObject (\n" +
                    "  id VARCHAR(8000) PRIMARY KEY,\n" +
                    "  visibility VARCHAR(8000) NOT NULL,\n" +
                    "  intColumn INTEGER NOT NULL,\n" +
                    "  nullableIntColumn INTEGER, \n" +
                    "  stringColumn TEXT NOT NULL, \n" +
                    "  nullableStringColumn TEXT, \n" +
                    "  jsonColumn TEXT NOT NULL, \n" +
                    "  nullableJsonColumn TEXT\n" +
                    ");";
            PreparedStatement stmt = conn.prepareStatement(sql);
            stmt.execute();
        } catch (SQLException e) {
            throw new SimpleOrmException("Could not create tables", e);
        }

        return session;
    }
}
