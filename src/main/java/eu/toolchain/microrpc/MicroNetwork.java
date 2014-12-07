package eu.toolchain.microrpc;

import eu.toolchain.async.AsyncFuture;

public interface MicroNetwork {
    AsyncFuture<MicroConnection> connect(MicroNetworkState state, String host, int port);

    MicroNetworkState setup() throws InterruptedException;

    long idleTimeout();
}
