package org.neolumin.simpleOrm;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.neolumin.simpleOrm.model.JettySession;
import org.eclipse.jetty.nosql.NoSqlSession;
import org.eclipse.jetty.nosql.NoSqlSessionManager;
import org.eclipse.jetty.server.SessionManager;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class SimpleOrmJettySessionManager extends NoSqlSessionManager {
    // Used by Jetty
    @SuppressWarnings("UnusedDeclaration")
    public static final Class TYPE = SessionManager.class;

    private static final int CACHE_MAX_SIZE = 50;
    private static final int CACHE_EXPIRE_MINUTES = 10;
    private static final String VISIBILITY_STRING = "";

    private final SimpleOrmSession simpleOrmSession;
    private final SimpleOrmContext simpleOrmContext;
    private final LoadingCache<String, Optional<JettySession>> cache;

    // Used by Jetty
    @SuppressWarnings("UnusedDeclaration")
    public SimpleOrmJettySessionManager(SimpleOrmSession simpleOrmSession) {
        this(simpleOrmSession, simpleOrmSession.createContext());
    }

    public SimpleOrmJettySessionManager(SimpleOrmSession simpleOrmSession, SimpleOrmContext simpleOrmContext) {
        this.simpleOrmSession = simpleOrmSession;
        this.simpleOrmContext = simpleOrmContext;

        this.cache = CacheBuilder.newBuilder()
                .maximumSize(CACHE_MAX_SIZE)
                .expireAfterWrite(CACHE_EXPIRE_MINUTES, TimeUnit.MINUTES)
                .build(new CacheLoader<String, Optional<JettySession>>() {
                    @Override
                    public Optional<JettySession> load(@SuppressWarnings("NullableProblems") String clusterId) throws Exception {
                        return Optional.fromNullable(getSimpleOrmSession().findById(JettySession.class, clusterId, getSimpleOrmContext()));
                    }
                });
    }

    @Override
    protected NoSqlSession loadSession(String clusterId) {
        Optional<JettySession> row = cache.getUnchecked(clusterId);
        if (!row.isPresent()) {
            return null;
        }

        JettySession jettySession = row.get();
        NoSqlSession session = new NoSqlSession(
                this,
                jettySession.getCreated(),
                jettySession.getAccessed(),
                jettySession.getClusterId(),
                jettySession.getVersion());
        setData(session, jettySession.getData());
        session.didActivate();

        return session;
    }

    @Override
    protected Object save(NoSqlSession session, Object version, boolean activateAfterSave) {
        session.willPassivate();

        if (session.isValid()) {
            boolean isNew = false;
            JettySession row;

            Optional<JettySession> optionalRow = cache.getUnchecked(session.getClusterId());

            if (!optionalRow.isPresent()) {
                // new session
                isNew = true;
                row = new JettySession(session.getClusterId(), session.getCreationTime());
                cache.put(session.getClusterId(), Optional.of(row));
                version = 0;
            } else {
                // existing session
                row = optionalRow.get();
                version = ((Number) version).longValue() + 1;
            }
            row.setVersion(((Number) version).longValue());
            row.setAccessed(session.getAccessed());

            Map<String, Object> data = row.getData();
            Set<String> attributesToSave = session.takeDirty();
            if (isNew || isSaveAllAttributes()) {
                attributesToSave.addAll(session.getNames());
            }
            for (String name : attributesToSave) {
                data.put(name, session.getAttribute(name));
            }

            simpleOrmSession.save(row, VISIBILITY_STRING, getSimpleOrmContext());
        } else {
            // invalid session
            simpleOrmSession.delete(JettySession.class, session.getClusterId(), getSimpleOrmContext());
            cache.invalidate(session.getClusterId());
        }

        if (activateAfterSave) {
            session.didActivate();
        }

        return version;
    }

    @Override
    protected Object refresh(NoSqlSession session, Object version) {
        Optional<JettySession> optRow = cache.getUnchecked(session.getClusterId());

        if (version != null) {
            if (optRow.isPresent()) {
                Long savedVersion = optRow.get().getVersion();
                if (savedVersion != null && savedVersion == ((Number) version).longValue()) {
                    // refresh not required
                    return version;
                }
            }
        }


        if (!optRow.isPresent()) {
            session.invalidate();
            return null;
        }

        JettySession row = optRow.get();
        session.willPassivate();
        session.clearAttributes();
        setData(session, row.getData());

        row.setAccessed(System.currentTimeMillis());
        simpleOrmSession.save(row, VISIBILITY_STRING, getSimpleOrmContext());

        session.didActivate();

        return version;
    }

    @Override
    protected boolean remove(NoSqlSession session) {
        Optional<JettySession> optRow = cache.getUnchecked(session.getClusterId());

        if (optRow.isPresent()) {
            simpleOrmSession.delete(JettySession.class, optRow.get().getId(), getSimpleOrmContext());
            cache.invalidate(session.getClusterId());
            return true;
        } else {
            return false;
        }
    }

    @Override
    protected void update(NoSqlSession session, String newClusterId, String newNodeId) throws Exception {
        // TODO
    }

    private void setData(NoSqlSession session, Map<String, Object> data) {
        for (Map.Entry<String, Object> col : data.entrySet()) {
            String name = col.getKey();
            Object value = col.getValue();

            session.doPutOrRemove(name, value);
            session.bindValue(name, value);
        }
    }

    public SimpleOrmSession getSimpleOrmSession() {
        return simpleOrmSession;
    }

    public SimpleOrmContext getSimpleOrmContext() {
        return simpleOrmContext;
    }
}
