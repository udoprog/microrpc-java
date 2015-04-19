package eu.toolchain.microrpc;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.microrpc.messages.MicroMessage;
import eu.toolchain.serializer.Serializer;

/**
 * Indicated a connection to another peer (either incoming or outgoing).
 *
 * @author udoprog
 */
public interface MicroConnection extends AutoCloseable {
    public static enum Direction {
        INCOMING, OUTGOING
    }

    /**
     * Send a request for the default version endpoint.
     *
     * This is equivalent of {@code request(endpoint, responseSerializer, null)}.
     *
     * @see #request(String, Serializer, Integer)
     */
    public <R> AsyncFuture<R> request(String endpoint, Serializer<R> result);

    /**
     * Send a request which has no body.
     *
     * This is equivalent of {@code request(endpoint, result, version, null, null)}.
     *
     * @see #request(String, Serializer, Integer, Object, Serializer)
     */
    public <R> AsyncFuture<R> request(String endpoint, Serializer<R> result, Integer version);

    /**
     * Send a request.
     *
     * @param endpoint Name of endpoint.
     * @param result Serializer of the result.
     * @param version Version of the receivning endpoint. Using {@code null} indicates that the default version endpoint
     *            should be called.
     * @param bodyValue Body of the request. Using {@code null} indicates an empty body.
     * @param body Serializer of the body. Using {@code null} indicates an empty body.
     * @param <B> Type of body.
     * @param <R> Type of result.
     * @return A future that will be resolved with the result of the request.
     */
    public <B, R> AsyncFuture<R> request(String endpoint, Serializer<R> result, Integer version, B bodyValue,
            Serializer<B> body);

    /**
     * Send a response.
     *
     * This will assert that the channel is flushed.
     *
     * @param message Message to send.
     * @return Future indicating if message was successfully sent.
     */
    public AsyncFuture<Void> send(MicroMessage message);

    /**
     * Close the connection.
     */
    @Override
    public void close();

    /**
     * Access the close future of this connection, this future will be invoked when this connection is closed.
     *
     * @return
     */
    public AsyncFuture<Void> closeFuture();

    /**
     * Check if this connection was requested to be closed with {@code #close()}.
     *
     * @return {@code true} if it was explicitly closed, {@code false} otherwise.
     */
    public boolean isCloseRequested();

    public void retain();

    public boolean release();

    public int refCount();

    public boolean isReady();
}