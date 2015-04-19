package eu.toolchain.microrpc;

import eu.toolchain.async.AsyncFuture;

public interface MicroBinding {
    /**
     * Close the given binding and all connections associated with it.
     */
    public AsyncFuture<Void> close();
}