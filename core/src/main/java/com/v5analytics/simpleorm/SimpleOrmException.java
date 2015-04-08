package com.v5analytics.simpleorm;

public class SimpleOrmException extends RuntimeException {
    private final String message;
    private final Throwable cause;

    public SimpleOrmException(String message, Throwable cause) {
        this.message = message;
        this.cause = cause;
    }

    public SimpleOrmException(String message) {
        this(message, null);
    }

    @Override
    public String getMessage() {
        return message;
    }

    @Override
    public Throwable getCause() {
        return cause;
    }

    @Override
    public String toString() {
        return getMessage();
    }
}
