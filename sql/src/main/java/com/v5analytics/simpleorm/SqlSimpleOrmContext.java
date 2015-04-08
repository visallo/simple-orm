package com.v5analytics.simpleorm;

public class SqlSimpleOrmContext extends SimpleOrmContext {
    private final String[] authorizations;

    public SqlSimpleOrmContext(String[] authorizations) {
        this.authorizations = authorizations;
    }

    public String[] getAuthorizations() {
        return authorizations;
    }
}
