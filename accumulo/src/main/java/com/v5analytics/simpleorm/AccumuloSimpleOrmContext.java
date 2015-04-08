package com.v5analytics.simpleorm;

import org.apache.accumulo.core.security.Authorizations;

public class AccumuloSimpleOrmContext extends SimpleOrmContext {
    private final Authorizations authorizations;

    public AccumuloSimpleOrmContext(Authorizations authorizations) {
        this.authorizations = authorizations;
    }

    public Authorizations getAuthorizations() {
        return authorizations;
    }
}
