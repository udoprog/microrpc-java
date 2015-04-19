package eu.toolchain.microrpc;

import java.util.UUID;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.microrpc.messages.MicroMessage;
import eu.toolchain.microrpc.timer.MicroTimer;

public interface MicroNetwork {
    AsyncFuture<MicroConnection> connect(String host, int port);

    AsyncFuture<Void> bind();

    AsyncFuture<Void> close();

    MicroTimer timer();

    boolean isReady();

    PendingRequestContext createPending(MicroConnection connection, UUID requestId,
            ResolvableFuture<MicroMessage> future);

    public static interface PendingRequestContext {
        void stop();
    }
}