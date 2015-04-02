package org.neolumin.simpleOrm;

import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AccumuloTest extends TestBase {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloTest.class);
    private String instanceName;
    private String zkServerNames;
    private String user = "root";
    private String password = "password";
    private File tempDir;
    private MiniAccumuloCluster accumulo;

    @Before
    @Override
    public void before() throws Exception {
        ensureAccumuloIsStarted();
        super.before();
    }

    private void ensureAccumuloIsStarted() {
        try {
            start();
        } catch (Exception e) {
            throw new SimpleOrmException("Failed to start Accumulo mini cluster", e);
        }
    }

    public void start() throws IOException, InterruptedException {
        if (accumulo != null) {
            return;
        }

        LOGGER.info("Starting accumulo");

        tempDir = File.createTempFile("accumulo-simple-orm-test-", Long.toString(System.nanoTime()));
        tempDir.delete();
        tempDir.mkdir();
        LOGGER.info("writing to: " + tempDir);

        MiniAccumuloConfig miniAccumuloConfig = new MiniAccumuloConfig(tempDir, password);
        accumulo = new MiniAccumuloCluster(miniAccumuloConfig);
        accumulo.start();

        zkServerNames = accumulo.getZooKeepers();
        instanceName = accumulo.getInstanceName();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    AccumuloTest.this.stop();
                } catch (Exception e) {
                    System.out.println("Failed to stop Accumulo test cluster");
                }
            }
        });
    }

    protected void stop() throws IOException, InterruptedException {
        if (accumulo != null) {
            LOGGER.info("Stopping accumulo");
            accumulo.stop();
            accumulo = null;
        }
        tempDir.delete();
    }

    @After
    @Override
    public void after() throws Exception {
        super.after();
    }

    @Override
    protected SimpleOrmSession createSession() {
        AccumuloSimpleOrmSession session = new AccumuloSimpleOrmSession();
        Map<String, Object> properties = new HashMap<>();
        properties.put(AccumuloSimpleOrmSession.ACCUMULO_INSTANCE_NAME, instanceName);
        properties.put(AccumuloSimpleOrmSession.ZK_SERVER_NAMES, zkServerNames);
        properties.put(AccumuloSimpleOrmSession.ACCUMULO_USER, user);
        properties.put(AccumuloSimpleOrmSession.ACCUMULO_PASSWORD, password);
        session.init(properties);
        return session;
    }
}
