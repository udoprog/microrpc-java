package eu.toolchain.microrpc;

import lombok.RequiredArgsConstructor;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.microrpc.messages.MicroMessage;
import eu.toolchain.microrpc.timer.TaskSchedule;

/**
 * Requests that are being processed locally.
 */
@RequiredArgsConstructor
public final class MicroProcessingRequest {
    /**
     * Future to resolve when the request has been processed.
     */
    private final AsyncFuture<MicroMessage> future;

    private final TaskSchedule task;

    public void fail(Exception exception) {
        task.cancel();
        future.fail(new Exception("Request timed out"));
    }
}