package eu.toolchain.microrpc;

import java.net.SocketAddress;
import java.util.UUID;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.microrpc.messages.MicroMessage;
import eu.toolchain.microrpc.timer.MicroTimer;

public interface MicroNetwork {
    AsyncFuture<MicroConnection> connect(SocketAddress address, UUID localId, ConnectionHandler handler);

    AsyncFuture<MicroBinding> bind(SocketAddress address, UUID localId, ConnectionHandler acceptor);

    MicroTimer timer();

    PendingRequestContext createPending(MicroConnection connection, UUID requestId,
            ResolvableFuture<MicroMessage> future);

    public static interface ConnectionHandler {
        public void ready(MicroConnection c);

        public void lost(MicroConnection c);
    }

    public static interface PendingRequestContext {
        void stop();
    }
}