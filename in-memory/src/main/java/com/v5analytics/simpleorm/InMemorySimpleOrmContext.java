package com.v5analytics.simpleorm;

public class InMemorySimpleOrmContext extends SimpleOrmContext {
    private final String[] authorizations;

    public InMemorySimpleOrmContext(String[] authorizations) {
        this.authorizations = authorizations;
    }

    public String[] getAuthorizations() {
        return authorizations;
    }
}

