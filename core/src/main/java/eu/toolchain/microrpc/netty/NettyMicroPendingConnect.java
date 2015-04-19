package eu.toolchain.microrpc.netty;

import lombok.RequiredArgsConstructor;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.microrpc.MicroConnection;
import eu.toolchain.microrpc.timer.MicroTimer;

/**
 * A single, pending connection.
 */
@RequiredArgsConstructor
public final class NettyMicroPendingConnect {
    private final ResolvableFuture<MicroConnection> future;
    private final MicroTimer.TaskSchedule timeout;

    public void resolve(NettyMicroConnection c) {
        future.resolve(c);
        timeout.cancel();
    }
}