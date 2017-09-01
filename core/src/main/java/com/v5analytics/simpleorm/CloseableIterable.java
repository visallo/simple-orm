package com.v5analytics.simpleorm;

import java.io.Closeable;

public interface CloseableIterable<T> extends Iterable<T>, Closeable, AutoCloseable {
    @Override
    CloseableIterator<T> iterator();
}
