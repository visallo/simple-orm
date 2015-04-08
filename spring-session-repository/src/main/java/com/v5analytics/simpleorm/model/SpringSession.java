package com.v5analytics.simpleorm.model;

import com.v5analytics.simpleorm.Entity;
import com.v5analytics.simpleorm.Field;
import com.v5analytics.simpleorm.Id;
import org.springframework.session.ExpiringSession;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Entity(tableName = "springSession")
public class SpringSession implements ExpiringSession {
    private static final int DEFAULT_MAX_INACTIVE_INTERVAL_SECONDS = 1800;

    @Id
    private String id;

    @Field
    private Map<String, Object> attributes;

    @Field
    private long creationTime;

    @Field
    private long lastAccessedTime;

    @Field
    private int maxInactiveIntervalSeconds;

    public static SpringSession create() {
        SpringSession session = new SpringSession();
        session.setId(UUID.randomUUID().toString());
        session.setAttributes(new HashMap<String, Object>());
        session.setCreationTime(System.currentTimeMillis());
        session.setLastAccessedTime(session.getCreationTime());
        session.setMaxInactiveIntervalInSeconds(DEFAULT_MAX_INACTIVE_INTERVAL_SECONDS);
        return session;
    }

    // Used by SimpleOrm to create instance
    protected SpringSession() {
    }

    public String getId() {
        return id;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T getAttribute(String attributeName) {
        return (T) attributes.get(attributeName);
    }

    @Override
    public Set<String> getAttributeNames() {
        return attributes.keySet();
    }

    @Override
    public void setAttribute(String attributeName, Object attributeValue) {
        if (attributeValue == null) {
            removeAttribute(attributeName);
        } else {
            attributes.put(attributeName, attributeValue);
        }
    }

    @Override
    public void removeAttribute(String attributeName) {
        attributes.remove(attributeName);
    }

    @Override
    public long getCreationTime() {
        return creationTime;
    }

    @Override
    public long getLastAccessedTime() {
        return lastAccessedTime;
    }

    @Override
    public void setMaxInactiveIntervalInSeconds(int maxInactiveIntervalSeconds) {
        this.maxInactiveIntervalSeconds = maxInactiveIntervalSeconds;
    }

    @Override
    public int getMaxInactiveIntervalInSeconds() {
        return maxInactiveIntervalSeconds;
    }

    @Override
    public boolean isExpired() {
        return isExpired(System.currentTimeMillis());
    }

    protected boolean isExpired(long now) {
        return maxInactiveIntervalSeconds >= 0 &&
                now - TimeUnit.SECONDS.toMillis(maxInactiveIntervalSeconds) >= lastAccessedTime;
    }

    protected void setId(String id) {
        this.id = id;
    }

    protected void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

    protected void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    protected void setLastAccessedTime(long lastAccessedTime) {
        this.lastAccessedTime = lastAccessedTime;
    }
}
