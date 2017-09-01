package com.v5analytics.simpleorm;

import java.io.Closeable;
import java.util.Iterator;

public interface CloseableIterator<T> extends Iterator<T>, Closeable, AutoCloseable {
}
