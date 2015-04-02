package org.neolumin.simpleOrm;

public class InMemoryTest extends TestBase {
    @Override
    protected SimpleOrmSession createSession() {
        return new InMemorySimpleOrmSession();
    }
}
