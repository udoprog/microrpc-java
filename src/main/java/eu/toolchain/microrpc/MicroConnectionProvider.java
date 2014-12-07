package eu.toolchain.microrpc;

import java.util.UUID;

import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.microrpc.messages.MicroMessage;

public interface MicroConnectionProvider {
    public static interface PendingRequestContext {
        void stop();
    }

    public PendingRequestContext createPending(MicroConnection c, UUID requestId, ResolvableFuture<MicroMessage> future);
}
