package eu.toolchain.microrpc;

import java.util.UUID;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.microrpc.messages.MicroMessage;

public interface MicroReceiverProvider {
    /**
     * Handle a request.
     *
     * @param c Connection from where the request originated.
     * @param m The corresponding version mapping of the request.
     * @param id The id of the request.
     * @param responseFuture A future that when resolved indicates that a response for the request has been processed.
     * @throws Exception If a request could not be handled.
     */
    void request(MicroConnection c, MicroVersionMapping m, UUID id, AsyncFuture<MicroMessage> responseFuture)
            throws Exception;

    /**
     * Get a pending request.
     *
     * @param id Id of the pending request.
     * @return A {@code MuPendingRequest} if it exists, {@code null} otherwise.
     */
    MicroPendingRequest pending(UUID id);

    /**
     * Remove a processing request with the given id.
     *
     * @param id Id of the request to remove.
     * @return A {@code MuProcessingRequest} if it exists, {@code null} otherwise.
     */
    MicroProcessingRequest removeProcessing(UUID id);

    /**
     * Indicate that a request with the given id received a keep-alive message.
     *
     * @param requestId Id of the request that received a keep-alive message.
     */
    void keepAlive(UUID requestId);
}