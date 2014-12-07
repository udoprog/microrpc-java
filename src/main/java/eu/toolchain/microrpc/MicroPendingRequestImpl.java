package eu.toolchain.microrpc;

import java.util.Date;

import lombok.RequiredArgsConstructor;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.microrpc.messages.MicroMessage;
import eu.toolchain.microrpc.timer.TaskSchedule;

/**
 * Requests that are being processed remotely.
 */
@RequiredArgsConstructor
public final class MicroPendingRequestImpl implements MicroPendingRequest {
    /**
     * Time when request was sent.
     *
     * TODO: Use for something?
     */
    @SuppressWarnings("unused")
    private final Date sent;

    /**
     * Future to resolve when a response is available.
     */
    private final ResolvableFuture<MicroMessage> future;

    /**
     * Pending timeout for this task.
     */
    private final TaskSchedule timeout;

    @Override
    public void resolve(MicroMessage value) {
        future.resolve(value);
    }

    @Override
    public void delayTimeout(long delay) {
        timeout.delayUntil(System.currentTimeMillis() + delay);
    }

    @Override
    public void cancelTimeout() {
        timeout.cancel();
    }
}