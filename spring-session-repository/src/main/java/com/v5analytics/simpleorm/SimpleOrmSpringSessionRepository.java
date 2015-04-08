package com.v5analytics.simpleorm;

import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.v5analytics.simpleorm.model.SpringSession;
import org.springframework.session.SessionRepository;

import java.util.concurrent.TimeUnit;

public class SimpleOrmSpringSessionRepository implements SessionRepository<SpringSession> {
    private static final int CACHE_MAX_SIZE = 50;
    private static final int CACHE_EXPIRE_MINUTES = 10;
    private static final String VISIBILITY_STRING = "";

    private Integer defaultMaxInactiveInterval = null;
    private final SimpleOrmSession simpleOrmSession;
    private final SimpleOrmContext simpleOrmContext;
    private final LoadingCache<String, Optional<SpringSession>> cache;

    public SimpleOrmSpringSessionRepository(SimpleOrmSession simpleOrmSession) {
        this(simpleOrmSession, simpleOrmSession.createContext());
    }

    public SimpleOrmSpringSessionRepository(final SimpleOrmSession simpleOrmSession, final SimpleOrmContext simpleOrmContext) {
        this.simpleOrmSession = simpleOrmSession;
        this.simpleOrmContext = simpleOrmContext;

        this.cache = CacheBuilder.newBuilder()
                .maximumSize(CACHE_MAX_SIZE)
                .expireAfterWrite(CACHE_EXPIRE_MINUTES, TimeUnit.MINUTES)
                .build(new CacheLoader<String, Optional<SpringSession>>() {
                    @Override
                    public Optional<SpringSession> load(String id) throws Exception {
                        return Optional.fromNullable(simpleOrmSession.findById(SpringSession.class, id, simpleOrmContext));
                    }
                });
    }

    public void setDefaultMaxInactiveInterval(int defaultMaxInactiveInterval) {
        this.defaultMaxInactiveInterval = defaultMaxInactiveInterval;
    }

    @Override
    public SpringSession createSession() {
        SpringSession springSession = SpringSession.create();
        if(defaultMaxInactiveInterval != null) {
            springSession.setMaxInactiveIntervalInSeconds(defaultMaxInactiveInterval);
        }
        return springSession;
    }

    @Override
    public void save(SpringSession springSession) {
        simpleOrmSession.save(springSession, VISIBILITY_STRING, simpleOrmContext);
        cache.put(springSession.getId(), Optional.of(springSession));
    }

    @Override
    public SpringSession getSession(String id) {
        return cache.getUnchecked(id).orNull();
    }

    @Override
    public void delete(String id) {
        simpleOrmSession.delete(SpringSession.class, id, simpleOrmContext);
        cache.invalidate(id);
    }
}
