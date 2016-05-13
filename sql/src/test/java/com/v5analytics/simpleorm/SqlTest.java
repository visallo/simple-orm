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
        properties.put("simpleOrm.sql.jdbcUrl", connectionString);
        properties.put("simpleOrm.sql.username", "sa");
        properties.put("simpleOrm.sql.password", "");
        session.init(properties);
        return session;
    }
}
