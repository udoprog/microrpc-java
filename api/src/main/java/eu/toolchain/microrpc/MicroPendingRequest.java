package eu.toolchain.microrpc;

import eu.toolchain.microrpc.messages.MicroMessage;


/**
 * Requests that are being processed remotely.
 */
public interface MicroPendingRequest {
    public void resolve(MicroMessage value);

    public void delayTimeout(long delay);

    public void cancelTimeout();
}