package eu.toolchain.microrpc;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import lombok.ToString;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Collector;
import eu.toolchain.async.FutureDone;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.async.Transform;
import eu.toolchain.microrpc.connection.PooledMicroConnection;
import eu.toolchain.microrpc.connection.ReconnectingMicroConnection;
import eu.toolchain.microrpc.connection.RetryingMicroConnection;
import eu.toolchain.microrpc.messages.MicroErrorMessage;
import eu.toolchain.microrpc.messages.MicroMessage;
import eu.toolchain.microrpc.messages.MicroRequestKeepAlive;
import eu.toolchain.microrpc.messages.MicroRequestTimeout;
import eu.toolchain.microrpc.serializer.MicroSerializers;
import eu.toolchain.microrpc.timer.Task;
import eu.toolchain.microrpc.timer.TaskSchedule;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;

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
 * |         ...        | (3) *more keep-alive sent*
 * |                    |
 * |<-------------------| (4) response sent (requestId=A)
 * </pre>
 *
 * Alternately, if keep-alive messages are missing for the period of an entire idle timeout, the following happens.
 *
 * <pre>
 * |------------------->| (1) request sent (id=A)
 * |                    |
 * |                    | (2) *no keep-alive messages*
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
@ToString(of = "network")
public class MicroServer implements AutoCloseable {
    private final AsyncFramework async;
    private final MicroSerializers models;
    private final MicroNetwork network;
    private final long retryBackoff;
    private final int maxRetries;

    /**
     * Pending requests.
     */
    private final ConcurrentHashMap<UUID, MicroPendingRequest> pendingRequests = new ConcurrentHashMap<>();

    /**
     * Locally processing requests.
     */
    private final ConcurrentHashMap<UUID, MicroProcessingRequest> processingRequests = new ConcurrentHashMap<>();

    /**
     * Mutable runtime state.
     *
     * If this is not set (check with {@link AtomicReference#get()}) the service is not running.
     */
    private final AtomicReference<MicroNetworkState> runtime = new AtomicReference<>();

    /**
     * The amount of pending retries.
     */
    private final AtomicInteger pendingRetries = new AtomicInteger();

    /**
     * Shared random pool.
     */
    private final Random random = new Random();

    /**
     * Connection provider implementation for this server instance.
     */
    private final MicroConnectionProvider connectionProvider = new MicroConnectionProvider() {
        @Override
        public PendingRequestContext createPending(final MicroConnection c, final UUID requestId,
                final ResolvableFuture<MicroMessage> future) {
            final MicroNetworkState s = state();

            final Date now = new Date();

            final TaskSchedule timeout = s.scheduleTimeout(new Task() {
                @Override
                public void run(TaskSchedule t) throws Exception {
                    future.fail(new Exception("Request timed out"));
                    c.send(new MicroRequestTimeout(requestId));
                }
            });

            final MicroPendingRequest p = new MicroPendingRequestImpl(now, future, timeout);

            pendingRequests.put(requestId, p);

            return new PendingRequestContext() {
                @Override
                public void stop() {
                    p.cancelTimeout();
                    pendingRequests.remove(requestId);
                }
            };
        }
    };

    /**
     * Receiver provider implementation for this server instance.
     */
    private final MicroReceiverProvider receiverProvider = new MicroReceiverProvider() {
        @Override
        public void request(final MicroConnection c, final MicroVersionMapping m, final UUID id,
                AsyncFuture<MicroMessage> responseFuture) throws Exception {
            // a response is already available, send it immediately.
            if (responseFuture.isDone()) {
                c.send(responseFuture.getNow());
                return;
            }

            // schedule keep-alive timer.
            final TaskSchedule keepalive = state().scheduleKeepAlive(new Task() {
                @Override
                public void run(TaskSchedule t) throws Exception {
                    c.send(new MicroRequestKeepAlive(id));
                }
            });

            // setup mapping for the processing request.
            processingRequests.put(id, new MicroProcessingRequest(responseFuture, keepalive));

            // listen for completion of the response future.
            responseFuture.on(new FutureDone<MicroMessage>() {
                public void finished() {
                    processingRequests.remove(id);
                    keepalive.cancel();
                }

                @Override
                public void failed(Throwable e) throws Exception {
                    finished();
                    c.send(new MicroErrorMessage(id, e.getMessage()));
                }

                @Override
                public void resolved(MicroMessage result) throws Exception {
                    finished();
                    c.send(result);
                }

                @Override
                public void cancelled() throws Exception {
                    finished();
                    c.send(new MicroErrorMessage(id, "request cancelled"));
                }
            });
        }

        @Override
        public MicroPendingRequest pending(UUID id) {
            return pendingRequests.get(id);
        }

        @Override
        public MicroProcessingRequest removeProcessing(UUID id) {
            return processingRequests.remove(id);
        }

        @Override
        public void keepAlive(UUID requestId) {
            final MicroPendingRequest pending = pending(requestId);

            // delay the timeout of the pending request, if a keep-alive message for it was received.
            if (pending != null)
                pending.delayTimeout(network.idleTimeout());
        }
    };

    private MicroServer(AsyncFramework async, SerializerFramework serializer, MicroRouter router,
            MicroNetworkConfig config, UUID localId, long retryBackoff, int maxRetries) {
        this.async = async;
        this.models = new MicroSerializers(serializer);
        final MicroReceiver receiver = new MicroReceiverImpl(receiverProvider, router);
        this.network = config.setup(async, models.message(), receiver, connectionProvider, localId);
        this.retryBackoff = retryBackoff;
        this.maxRetries = maxRetries;
    }

    public static Builder builder() {
        return new Builder();
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

        final MicroNetworkState state = state();

        return requestRetries(state, host, port, count, requestRetries, maxPendingRetries);
    }

    private AsyncFuture<MicroConnection> requestRetries(final MicroNetworkState state, String host, int port,
            int count, final int requestRetries, final int maxPendingRetries) {
        final AsyncFuture<MicroConnection> c = poolConnect(state, host, port, count);

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
                return new RetryingMicroConnection(async, c, retries, retryBackoff, state.timer(), pendingRetries,
                        maxPendingRetries);
            }
        });
    }

    private AsyncFuture<MicroConnection> poolConnect(final MicroNetworkState state, String host, int port, int count) {
        if (count == 1)
            return connect(state, host, port);

        final List<AsyncFuture<MicroConnection>> connections = new ArrayList<>(count);

        for (int i = 0; i < count; ++i)
            connections.add(connect(state, host, port));

        return async.collect(connections, new Collector<MicroConnection, MicroConnection>() {
            @Override
            public MicroConnection collect(Collection<MicroConnection> results) throws Exception {
                if (results.isEmpty())
                    throw new IllegalArgumentException("no connections");

                return new PooledMicroConnection(async, new ArrayList<>(results), random);
            }
        });
    }

    private AsyncFuture<MicroConnection> connect(final MicroNetworkState state, final String host, final int port) {
        final ReconnectingMicroConnection.Establish est = new ReconnectingMicroConnection.Establish() {
            @Override
            public AsyncFuture<MicroConnection> establish() {
                return network.connect(state, host, port);
            }
        };

        return est.establish().transform(new Transform<MicroConnection, MicroConnection>() {
            @Override
            public MicroConnection transform(MicroConnection c) throws Exception {
                return new ReconnectingMicroConnection(async, c, est, state.timer(), 1000, async.<Void> future());
            }
        });
    }

    public void start() throws Exception {
        final MicroNetworkState state = network.setup();

        if (!this.runtime.compareAndSet(null, state)) {
            state.shutdown();
            throw new IllegalStateException("Server already started");
        }
    }

    public void stop() throws Exception {
        final MicroNetworkState state = this.runtime.getAndSet(null);

        if (state == null)
            return;

        state.shutdown();
    }

    @Override
    public void close() throws Exception {
        stop();
    }

    public boolean isReady() {
        return this.runtime.get() != null;
    }

    /**
     * Access current server state.
     *
     * @return
     */
    private MicroNetworkState state() {
        final MicroNetworkState state = this.runtime.get();

        if (state == null)
            throw new IllegalStateException("server not started");

        return state;
    }

    public static MicroRouter router(MicroConfiguration... configurations) {
        final MicroRouter router = new MicroRouterImpl();

        for (final MicroConfiguration bootstrap : configurations)
            bootstrap.configure(router);

        return router;
    }

    public static class Builder {
        private static final long DEFAULT_RETRY_BACKOFF = 10;
        private static final int DEFAULT_MAX_RETRIES = 0;

        private AsyncFramework async;
        private SerializerFramework serializer;
        private MicroRouter router;
        private MicroNetworkConfig networkConfig;
        private UUID localId;
        private long retryBackoff = DEFAULT_RETRY_BACKOFF;
        private int maxRetries = DEFAULT_MAX_RETRIES;

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
         * Serializer framework to use.
         *
         * @param serializer Serializer framework to use.
         * @return This builder.
         */
        public Builder serializer(SerializerFramework serializer) {
            if (serializer == null)
                throw new IllegalArgumentException("serializer: must not be null");

            this.serializer = serializer;
            return this;
        }

        /**
         * Router to use.
         *
         * A router governs which endpoints are available. It is essentially mapping
         * <code>(endpoint, version) -> callback</code>, and their corresponding serializers.
         *
         * @param router Router to use.
         * @return This builder.
         */
        public Builder router(MicroRouter router) {
            if (router == null)
                throw new IllegalArgumentException("router: must not be null");

            this.router = router;
            return this;
        }

        /**
         * Network configuration to use.
         *
         * @param networkConfig Network configuration to use.
         * @return This builder.
         */
        public Builder networkConfig(MicroNetworkConfig networkConfig) {
            if (networkConfig == null)
                throw new IllegalArgumentException("networkConfig: must not be null");

            this.networkConfig = networkConfig;
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

        public MicroServer build() {
            if (async == null)
                throw new IllegalArgumentException("async: must be configured");

            if (serializer == null)
                throw new IllegalArgumentException("serializer: must be configured");

            if (networkConfig == null)
                throw new IllegalArgumentException("networkConfig: must be configured");

            if (router == null)
                throw new IllegalArgumentException("router: must be configured");

            final UUID localId = this.localId == null ? UUID.randomUUID() : this.localId;

            return new MicroServer(async, serializer, router, networkConfig, localId, retryBackoff, maxRetries);
        }
    }
}
