package eu.toolchain.microrpc;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Collector;
import eu.toolchain.async.LazyTransform;
import eu.toolchain.async.Transform;
import eu.toolchain.microrpc.MicroNetwork.ConnectionHandler;
import eu.toolchain.microrpc.connection.PooledMicroConnection;
import eu.toolchain.microrpc.connection.ReconnectingMicroConnection;
import eu.toolchain.microrpc.connection.RetryingMicroConnection;
import eu.toolchain.serializer.Serializer;

/**
 * A tiny, very manual, rpc service implementation built on netty.
 *
 * IPC happens over TCP, where MicroRPC maintains and re-uses connections as long as possible. The user-facing message
 * are all sent over a request-response paradigm. The service takes care to ensure that no one connection blocks for too
 * long by maintain local timeouts.
 *
 * Requests must implement the provided serialization framework, where {@link Serializer} is the primary class of
 * interaction. This allows for highly efficient RPC, since it (tries) to avoid copying memory as much as reasonable.
 *
 * As soon as both client and server has established a connection, they simultaneously send metadata to eachother. The
 * metadata contains the local id of the server, and is used to determine if a connection can be re-used or not.
 *
 * The lifecycle of a request is as follows.
 *
 * <pre>
 * C                    S
 * |------------------->| (1) request sent (id=A)
 * |                    |
 * |<-------------------| (2) keep-alive sent (requestId=A)
 * |                    |
 * |         ...        | (3) *heartbeats sent*
 * |                    |
 * |<-------------------| (4) response sent (requestId=A)
 * </pre>
 *
 * Alternately, if keep-alive messages are missing for the period of an entire idle timeout, the following happens.
 *
 * <pre>
 * |------------------->| (1) request sent (id=A)
 * |                    |
 * |                    | (2) *no heartbeats*
 * |                    |
 * |------------------->| (3) timeout sent (requestId=A)
 * |                    |
 * |<-------------------| (4) response sent (requestId=A), which will be ignored.
 * </pre>
 *
 * A request is sent (1), but no keep-alive messages reach the sending service (2). This causes the service to timeout
 * after the idle period and mark the request as failed, a timeout message is sent to the remote service (3).
 *
 * Even if a response is received (4) it is too late, since the Future associated with the request will already have
 * been marked as failed. It is therefore be ignored.
 *
 * This type of communication offers the following advantages on top of regular TCP.
 *
 * <ul>
 * <li>It can be detected when the remote service is no longer processing the request, but a connection is still
 * considered as up (e.g. due to badly crashed process, garbage collection, or misbehaving networks)</li>
 * <li>Communication is fully state-less and asynchronous, messages can be interleaved for efficient throughput.</li>
 * <li>Since connections are being kept alive, and re-used. Connection latency can mostly be avoided which has very
 * noticeable increases for global (>100ms RTT) user requests.</li>
 * </ul>
 *
 * <h1>TODO</h1>
 *
 * After serializing, messages can be further split up into frames to avoid that large messages hog all the available
 * bandwidth. Alternately, more connections could be established to each peer. Some of which could be used for smaller
 * frames.
 */
@Slf4j
@ToString(of = "network")
public class MicroServer implements AutoCloseable {
    private final AsyncFramework async;
    private final MicroNetwork network;
    private final SocketAddress address;
    private final UUID localId;

    /**
     * Mutable runtime state.
     *
     * If this is not set (check with {@link AtomicReference#get()}) the service is not running.
     */
    private final int maxRetries;

    private final long retryBackoff;

    /**
     * The amount of pending retries.
     */
    private final AtomicInteger pendingRetries = new AtomicInteger();

    /**
     * Shared random pool.
     */
    private final Random random = new Random();

    private final AtomicReference<MicroBinding> binding = new AtomicReference<>();

    private MicroServer(AsyncFramework async, MicroNetwork network, SocketAddress address, UUID localId,
            int maxRetries, long retryBackoff) {
        this.async = async;
        this.network = network;
        this.address = address;
        this.localId = localId;
        this.maxRetries = maxRetries;
        this.retryBackoff = retryBackoff;
    }

    /**
     * Establish a single connection.
     *
     * @param uri URI to connect to.
     * @return A future containing a newly established connection.
     */
    public AsyncFuture<MicroConnection> connect(URI uri) {
        return connect(uri, 1);
    }

    /**
     * Establish one, or several connections.
     *
     * @param uri URI to connect to.
     * @param count Number of connections to establish.
     * @return A future containing a newly established connection.
     */
    public AsyncFuture<MicroConnection> connect(URI uri, int count) {
        return connect(uri, count, 0, 0);
    }

    /**
     * Establish one, or several connections with request retrying.
     *
     * @param uri URI to connect to.
     * @param count Number of connections to establish.
     * @param requestRetries The amount of times a request should be retried until it is considered failed. {@code 0}
     *            means no retries.
     * @param maxPendingRetries Max allowed pending retries.
     * @return A future containing a newly established connection.
     */
    public AsyncFuture<MicroConnection> connect(URI uri, int count, int requestRetries, int maxPendingRetries) {
        return connect(uri.getHost(), uri.getPort(), count, requestRetries, maxPendingRetries);
    }

    /**
     * Establish one, or several connections.
     *
     * @param host Host to connect to.
     * @param port Port to connect to.
     * @param count Number of underlying connections to establish.
     * @param requestRetries The amount of times a request should be retried until it is considered failed. A value of
     *            {@code 0} or less disables retries.
     *
     * @return A AsyncFuture containing a newly established connection.
     */
    public AsyncFuture<MicroConnection> connect(final String host, final int port, final int count,
            final int requestRetries, final int maxPendingRetries) {
        if (host == null)
            throw new IllegalArgumentException("host must be specified");

        if (port < 0 || port >= 0xffff)
            throw new IllegalArgumentException("port must be specified and valid");

        if (count < 1)
            throw new IllegalArgumentException("size must be a positive number");

        return requestRetries(host, port, count, requestRetries, maxPendingRetries);
    }

    private AsyncFuture<MicroConnection> requestRetries(String host, int port, int count, final int requestRetries,
            final int maxPendingRetries) {
        final AsyncFuture<MicroConnection> c = poolConnect(host, port, count);

        final int retries;

        // either use a retry configuration for this request, or the globally configured one if available.
        if (requestRetries > 0) {
            retries = requestRetries;
        } else {
            if (maxRetries <= 0)
                return c;

            retries = maxRetries;
        }

        return c.transform(new Transform<MicroConnection, MicroConnection>() {
            @Override
            public MicroConnection transform(MicroConnection c) throws Exception {
                return new RetryingMicroConnection(async, c, retries, retryBackoff, network.timer(), pendingRetries,
                        maxPendingRetries);
            }
        });
    }

    private AsyncFuture<MicroConnection> poolConnect(String host, int port, int count) {
        if (count == 1)
            return connect(host, port);

        final List<AsyncFuture<MicroConnection>> connections = new ArrayList<>(count);

        for (int i = 0; i < count; ++i)
            connections.add(connect(host, port));

        return async.collect(connections, new Collector<MicroConnection, MicroConnection>() {
            @Override
            public MicroConnection collect(Collection<MicroConnection> results) throws Exception {
                if (results.isEmpty())
                    throw new IllegalArgumentException("no connections");

                return new PooledMicroConnection(async, new ArrayList<>(results), random);
            }
        });
    }

    private AsyncFuture<MicroConnection> connect(final String host, final int port) {
        final InetSocketAddress address = new InetSocketAddress(host, port);

        final ReconnectingMicroConnection.Establish est = new ReconnectingMicroConnection.Establish() {
            @Override
            public AsyncFuture<MicroConnection> establish() {
                return network.connect(address, localId, null);
            }
        };

        return est.establish().transform(new Transform<MicroConnection, MicroConnection>() {
            @Override
            public MicroConnection transform(MicroConnection c) throws Exception {
                return new ReconnectingMicroConnection(async, c, est, network.timer(), 1000, async.<Void> future());
            }
        });
    }

    public AsyncFuture<Void> start() throws Exception {
        MicroNetwork.ConnectionHandler handler = new ConnectionHandler() {
            @Override
            public void ready(MicroConnection c) {
                log.debug("connected: {}", c);
            }

            @Override
            public void lost(MicroConnection c) {
                log.debug("disconnected: {}");
            }
        };

        return network.bind(address, localId, handler).transform(new LazyTransform<MicroBinding, Void>() {
            @Override
            public AsyncFuture<Void> transform(MicroBinding result) throws Exception {
                if (binding.compareAndSet(null, result))
                    return async.resolved(null);

                return result.close();
            }
        });
    }

    public AsyncFuture<Void> stop() throws Exception {
        final MicroBinding b = binding.getAndSet(null);

        if (b == null)
            return async.resolved(null);

        return b.close();
    }

    /**
     * @see {@link AutoCloseable#close()}
     */
    @Override
    public void close() throws Exception {
        stop().get();
    }

    public boolean isReady() {
        return binding.get() != null;
    }

    public static MicroRouter router(MicroConfiguration... configurations) {
        final MicroRouter router = new MicroRouterImpl();

        for (final MicroConfiguration bootstrap : configurations)
            bootstrap.configure(router);

        return router;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private static final long DEFAULT_RETRY_BACKOFF = 10;
        private static final int DEFAULT_MAX_RETRIES = 0;
        public static SocketAddress DEFAULT_ADDRESS = new InetSocketAddress("127.0.0.1", 8100);

        private AsyncFramework async;
        private MicroNetwork network;
        private UUID localId;
        private long retryBackoff = DEFAULT_RETRY_BACKOFF;
        private int maxRetries = DEFAULT_MAX_RETRIES;
        private SocketAddress address = DEFAULT_ADDRESS;

        /**
         * The local id of this service.
         *
         * If not specified it will be generated randomly.
         *
         * @param localId The id of this service.
         * @return This builder.
         */
        public Builder localId(UUID localId) {
            if (localId == null)
                throw new IllegalArgumentException("localId: must not be null");

            this.localId = localId;
            return this;
        }

        /**
         * Async framework to use.
         *
         * @param async Async framework to use.
         * @return This builder.
         */
        public Builder async(AsyncFramework async) {
            if (async == null)
                throw new IllegalArgumentException("async: must not be null");

            this.async = async;
            return this;
        }

        /**
         * Network configuration to use.
         *
         * @param networkConfig Network configuration to use.
         * @return This builder.
         */
        public Builder network(MicroNetwork network) {
            if (network == null)
                throw new IllegalArgumentException("network: must not be null");

            this.network = network;
            return this;
        }

        /**
         * The initial amount of time that a connection will wait until retrying a request.
         *
         * This will exponentially increase with a factor of 2 for each attempt, until {@link #maxRetries} is reached.
         *
         * @param retryBackoff The initial retry back-off time to use.
         * @return This builder.
         */
        public Builder retryBackoff(long retryBackoff) {
            if (retryBackoff <= 0)
                throw new IllegalArgumentException("retryBackoff: must be positive");

            this.retryBackoff = retryBackoff;
            return this;
        }

        /**
         * Maximum amount of retries allowed for a failing request.
         *
         * This can also be configured on a per-request basis.
         *
         * @param maxRetries The maximum amount of retries to attempt for a failing request. Is disabled if set to
         *            {@code 0}.
         * @return This builder.
         */
        public Builder maxRetries(int maxRetries) {
            if (maxRetries < 0)
                throw new IllegalArgumentException("maxRetries: must be zero, or a positive number");

            this.maxRetries = 0;
            return this;
        }

        public Builder address(SocketAddress address) {
            if (address == null)
                throw new IllegalArgumentException("address: must not be null");

            this.address = address;
            return this;
        }

        public MicroServer build() {
            if (async == null)
                throw new IllegalArgumentException("async: must be configured");

            if (network == null)
                throw new IllegalArgumentException("network: must be configured");

            final UUID localId = this.localId == null ? UUID.randomUUID() : this.localId;

            return new MicroServer(async, network, address, localId, maxRetries, retryBackoff);
        }
    }
}
