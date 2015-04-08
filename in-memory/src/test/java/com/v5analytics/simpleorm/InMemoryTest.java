package com.v5analytics.simpleorm;

public class InMemoryTest extends TestBase {
    @Override
    protected SimpleOrmSession createSession() {
        return new InMemorySimpleOrmSession();
    }
}
