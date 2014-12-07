package eu.toolchain.microrpc;

import eu.toolchain.async.AsyncFuture;

public interface MicroEndPoint<Q, R> {
    AsyncFuture<R> query(Q query) throws Exception;
}
