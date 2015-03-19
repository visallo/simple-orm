package io.lumify.simpleOrm;

import org.eclipse.jetty.server.SessionManager;
import org.eclipse.jetty.server.session.SessionHandler;

public class SimpleOrmJettySessionHandler extends SessionHandler {
    public static final Class TYPE = SessionHandler.class;

    // Needed by Jetty
    @SuppressWarnings("UnusedDeclaration")
    public SimpleOrmJettySessionHandler() {
        super();
    }

    public SimpleOrmJettySessionHandler(SessionManager manager) {
        super(manager);
    }
}
