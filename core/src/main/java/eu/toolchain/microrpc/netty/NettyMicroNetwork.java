package eu.toolchain.microrpc.netty;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.compression.SnappyFrameDecoder;
import io.netty.handler.codec.compression.SnappyFrameEncoder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import lombok.extern.slf4j.Slf4j;

import com.google.common.io.ByteStreams;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;
import eu.toolchain.async.FutureFinished;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.async.Transform;
import eu.toolchain.microrpc.MicroConnection;
import eu.toolchain.microrpc.MicroNetwork;
import eu.toolchain.microrpc.MicroPendingRequest;
import eu.toolchain.microrpc.MicroPendingRequestImpl;
import eu.toolchain.microrpc.MicroProcessingRequest;
import eu.toolchain.microrpc.MicroRouter;
import eu.toolchain.microrpc.messages.MicroErrorMessage;
import eu.toolchain.microrpc.messages.MicroMessage;
import eu.toolchain.microrpc.messages.MicroMetadata;
import eu.toolchain.microrpc.messages.MicroRequest;
import eu.toolchain.microrpc.messages.MicroRequestHeartbeat;
import eu.toolchain.microrpc.messages.MicroRequestResponse;
import eu.toolchain.microrpc.messages.MicroRequestTimeout;
import eu.toolchain.microrpc.messages.MicroResponseMessage;
import eu.toolchain.microrpc.serializer.DataOutputSerialWriter;
import eu.toolchain.microrpc.serializer.MicroSerializers;
import eu.toolchain.microrpc.timer.MicroThreadTimer;
import eu.toolchain.microrpc.timer.MicroTimer;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;

@Slf4j
public class NettyMicroNetwork implements MicroNetwork {
    private final AsyncFramework async;
    private final Serializer<MicroMessage> messageSerializer;
    private final SocketAddress address;
    private final MicroRouter router;

    private final UUID localId;
    private final MicroTimer timer;

    private final EventLoopGroup parent;
    private final EventLoopGroup child;

    private final long idleTimeout;
    private final int maxFrameSize;
    private final long heartbeatTimeout;

    /**
     * Currently known remote ids.
     */
    private final ConcurrentHashMap<Channel, NettyMicroConnection> established = new ConcurrentHashMap<>();

    /**
     * Pending connects, awaiting metadata.
     */
    private final ConcurrentHashMap<Channel, NettyMicroPendingConnect> pendingConnects = new ConcurrentHashMap<>();

    /**
     * Initializer for incoming connections.
     */
    private final ChannelInitializer<? extends SocketChannel> incoming;

    /**
     * Initializer for outgoing connections.
     */
    private final ChannelInitializer<? extends SocketChannel> outgoing;

    private final NettyMicroMessageDecoder messageDecoder;

    private final AtomicReference<Channel> serverChannel = new AtomicReference<>();

    /**
     * Pending requests.
     */
    private final ConcurrentHashMap<UUID, MicroPendingRequest> pendingRequests = new ConcurrentHashMap<>();

    /**
     * Locally processing requests.
     */
    private final ConcurrentHashMap<UUID, MicroProcessingRequest> processingRequests = new ConcurrentHashMap<>();

    public NettyMicroNetwork(AsyncFramework async, Serializer<MicroMessage> messageSerializer, SocketAddress address,
            MicroRouter router, MicroTimer timer, EventLoopGroup parent, EventLoopGroup child, long idleTimeout,
            int maxFrameSize, long heartbeatTimeout, UUID localId) {
        this.async = async;
        this.messageSerializer = messageSerializer;
        this.address = address;
        this.router = router;

        this.timer = timer;
        this.parent = parent;
        this.child = child;

        this.idleTimeout = idleTimeout;
        this.maxFrameSize = maxFrameSize;
        this.heartbeatTimeout = heartbeatTimeout;

        this.localId = localId;

        this.incoming = initializer(handler(MicroConnection.Direction.INCOMING));
        this.outgoing = initializer(handler(MicroConnection.Direction.OUTGOING));

        this.messageDecoder = new NettyMicroMessageDecoder(messageSerializer);
    }

    private ChannelInitializer<SocketChannel> initializer(final SimpleChannelInboundHandler<MicroMessage> handler) {
        final ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<SocketChannel>() {
            @Override
            public void initChannel(SocketChannel ch) throws Exception {
                final ChannelPipeline p = ch.pipeline();

                p.addLast(new SnappyFrameDecoder(), new LengthFieldBasedFrameDecoder(maxFrameSize, 0, 4, 0, 4),
                        messageDecoder);

                p.addLast(new SnappyFrameEncoder(), new LengthFieldPrepender(4), new NettyMicroMessageEncoder(
                        messageSerializer));

                p.addLast(handler);
            }
        };

        return initializer;
    }

    public void request(final MicroConnection c, final MicroRouter.Mapping m, final UUID id,
            AsyncFuture<MicroMessage> responseFuture) throws Exception {
        // a response is already available, send it immediately.
        if (responseFuture.isDone()) {
            c.send(responseFuture.getNow());
            return;
        }

        // schedule keep-alive timer.
        final MicroTimer.TaskSchedule requestHeartbeat = timer.schedule("micro-heartbeat", heartbeatTimeout,
                new MicroTimer.Task() {
                    @Override
                    public void run(MicroTimer.TaskSchedule t) throws Exception {
                        c.send(new MicroRequestHeartbeat(id));
                        t.delay(heartbeatTimeout);
                    }
                });

        // setup mapping for the processing request.
        processingRequests.put(id, new MicroProcessingRequest(responseFuture, requestHeartbeat));

        // listen for completion of the response future.
        responseFuture.on(new FutureDone<MicroMessage>() {
            public void finished() {
                processingRequests.remove(id);
                requestHeartbeat.cancel();
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

    public MicroPendingRequest pending(UUID id) {
        return pendingRequests.get(id);
    }

    public MicroProcessingRequest removeProcessing(UUID id) {
        return processingRequests.remove(id);
    }

    public void heartbeat(UUID requestId) {
        final MicroPendingRequest pending = pending(requestId);

        // delay the timeout of the pending request.
        if (pending != null)
            pending.delayTimeout(idleTimeout);
    }

    private final Map<Class<? extends MicroMessage>, MessageHandle<? extends MicroMessage>> handles = new HashMap<>();

    private static interface MessageHandle<T> {
        void handle(MicroConnection c, T message) throws Exception;
    }

    private <T extends MicroMessage> void handle(Class<? extends T> type, MessageHandle<T> handle) {
        handles.put(type, handle);
    }

    {
        handle(MicroRequest.class, new MessageHandle<MicroRequest>() {
            @Override
            public void handle(final MicroConnection c, MicroRequest request) throws IOException {
                final MicroRouter.Mapping m = router.resolve(request.getVersion());

                if (m == null) {
                    c.send(new MicroErrorMessage(request.getId(), request.toString()
                            + ": no endpoint registered for version"));
                    return;
                }

                try {
                    request(c, m, request.getId(), callHandle(m, request));
                } catch (Exception e) {
                    c.send(new MicroErrorMessage(request.getId(), e.getMessage()));
                }
            }
        });

        handle(MicroRequestHeartbeat.class, new MessageHandle<MicroRequestHeartbeat>() {
            @Override
            public void handle(MicroConnection c, MicroRequestHeartbeat heartbeat) throws Exception {
                heartbeat(heartbeat.getRequestId());
            }
        });

        handle(MicroRequestTimeout.class, new MessageHandle<MicroRequestTimeout>() {
            @Override
            public void handle(MicroConnection c, MicroRequestTimeout timeout) throws Exception {
                final MicroProcessingRequest processing = removeProcessing(timeout.getRequestId());

                if (processing != null)
                    processing.fail(new Exception("request timed out"));
            }
        });

        final MessageHandle<MicroResponseMessage> response = new MessageHandle<MicroResponseMessage>() {
            @Override
            public void handle(MicroConnection c, MicroResponseMessage response) throws Exception {
                final MicroPendingRequest p = pending(response.requestId());

                if (p != null)
                    p.resolve(response);
            }
        };

        handle(MicroRequestResponse.class, response);
        handle(MicroErrorMessage.class, response);
    }

    /**
     * Handle for receiving all other state-less messages.
     */
    public void receive(final MicroConnection c, final MicroMessage message) throws Exception {
        if (log.isDebugEnabled())
            log.debug("<= {} {}", c, message);

        @SuppressWarnings("unchecked")
        final MessageHandle<MicroMessage> handle = (MessageHandle<MicroMessage>) handles.get(message.getClass());

        if (handle == null)
            throw new IllegalArgumentException("no handler for message: " + message);

        handle.handle(c, message);
    }

    private AsyncFuture<MicroMessage> callHandle(final MicroRouter.Mapping m, final MicroRequest request)
            throws Exception {
        final MicroRouter.EndpointHelper<?, ?> handle = m.endpoint(request.getEndpoint());

        if (handle == null)
            throw new IllegalArgumentException(request + ": no such endpoint");

        final Object query = handle.deserialize(request.getBody());

        final AsyncFuture<Object> response = handle.query(query);

        if (response == null)
            throw new IllegalArgumentException(request + ": handle returned null response");

        return response.transform(transformResponse(handle, request.getId())).error(errorResponse(m, request));
    }

    private Transform<Throwable, MicroMessage> errorResponse(final MicroRouter.Mapping m, final MicroRequest request) {
        return new Transform<Throwable, MicroMessage>() {
            @Override
            public MicroMessage transform(Throwable e) throws Exception {
                log.error(request + ": request failed", e);
                return new MicroErrorMessage(request.getId(), e.getMessage());
            }
        };
    }

    private Transform<Object, MicroMessage> transformResponse(final MicroRouter.EndpointHelper<?, ?> handle,
            final UUID id) {
        return new Transform<Object, MicroMessage>() {
            @Override
            public MicroMessage transform(Object result) throws Exception {
                final DataOutputSerialWriter out = new DataOutputSerialWriter(ByteStreams.newDataOutput());
                handle.serialize(out, result);
                return handle.result(id, out.toByteArray());
            }
        };
    }

    /**
     * Build a {@link ChannelInitializer} for incoming connections.
     */
    private SimpleChannelInboundHandler<MicroMessage> handler(final MicroConnection.Direction direction) {
        return new SimpleChannelInboundHandler<MicroMessage>() {
            @Override
            public void channelActive(ChannelHandlerContext ctx) throws Exception {
                ctx.channel().writeAndFlush(new MicroMetadata(localId));
            }

            @Override
            public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
                log.error("Error in channel, closing", cause);
                ctx.close();
            }

            @Override
            public boolean isSharable() {
                return true;
            }

            @Override
            protected void channelRead0(ChannelHandlerContext ctx, MicroMessage message) throws Exception {
                final MicroConnection c = established.get(ctx.channel());

                if (c == null) {
                    earlyReceive(ctx.channel(), direction, message);
                    return;
                }

                receive(c, message);
            }
        };
    }

    public PendingRequestContext createPending(final MicroConnection c, final UUID requestId,
            final ResolvableFuture<MicroMessage> future) {
        final Date now = new Date();

        final MicroTimer.TaskSchedule timeout = timer.schedule("micro-idle-timeout", idleTimeout,
                new MicroTimer.Task() {
                    @Override
                    public void run(MicroTimer.TaskSchedule t) throws Exception {
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

    /**
     * Handle for receiving metadata which is an early protocol stage.
     */
    private AsyncFuture<?> earlyReceive(Channel ch, MicroConnection.Direction d, final MicroMessage m) throws Exception {
        if (!(m instanceof MicroMetadata))
            throw new IllegalStateException("Expected metadata, but got: " + m);

        return resolveMetadata(ch, d, (MicroMetadata) m);
    }

    private AsyncFuture<?> resolveMetadata(final Channel ch, final MicroConnection.Direction d, final MicroMetadata m) {
        final NettyMicroConnection c = new NettyMicroConnection(async, this, d, m.getRemoteId(), ch);

        established.put(ch, c);

        ch.closeFuture().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                established.remove(ch);
            }
        });

        if (log.isDebugEnabled())
            log.debug("<= (M) {} {}", c, m);

        final NettyMicroPendingConnect pendingConnect = pendingConnects.remove(ch);

        // resolve a pending connect
        if (pendingConnect != null) {
            pendingConnect.resolve(c);

            if (log.isDebugEnabled())
                log.debug("connected pending {}", c);
        }

        return async.resolved(null);
    }

    @Override
    public AsyncFuture<Void> bind() {
        final ServerBootstrap server = new ServerBootstrap();
        server.channel(EpollServerSocketChannel.class).group(parent, child).childHandler(incoming);
        final ChannelFuture bind = server.bind(address);

        final ResolvableFuture<Void> future = async.future();

        bind.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture f) throws Exception {
                if (!f.isSuccess()) {
                    future.fail(f.cause());
                    return;
                }

                final Channel c = f.channel();

                if (serverChannel.compareAndSet(null, c))
                    return;

                c.close();
                future.fail(new IllegalStateException("already bound to address"));
                return;
            }
        });

        return future;
    }

    @Override
    public AsyncFuture<Void> close() {
        final Channel serverChannel = this.serverChannel.getAndSet(null);

        if (serverChannel == null)
            throw new IllegalStateException("server channel not set");

        final ResolvableFuture<Void> future = async.future();

        serverChannel.close().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture f) throws Exception {
                if (!f.isSuccess()) {
                    future.fail(f.cause());
                    return;
                }

                future.resolve(null);
            }
        });

        return future;
    }

    @Override
    public boolean isReady() {
        return serverChannel.get() != null;
    }

    @Override
    public MicroTimer timer() {
        return timer;
    }

    @Override
    public AsyncFuture<MicroConnection> connect(final String host, final int port) {
        final InetSocketAddress remoteAddress = new InetSocketAddress(host, port);
        final Bootstrap b = new Bootstrap();

        b.channel(EpollSocketChannel.class);
        b.group(child);
        b.handler(outgoing);

        final ChannelFuture connectFuture = b.connect(remoteAddress);

        final Channel ch = connectFuture.channel();

        final ResolvableFuture<MicroConnection> result = async.future();

        connectFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess())
                    result.fail(future.cause());
            }
        });

        final MicroTimer.TaskSchedule timeout = timer.schedule(idleTimeout, new MicroTimer.Task() {
            @Override
            public void run(MicroTimer.TaskSchedule t) throws Exception {
                ch.close();
                result.fail(new Exception("Connection timed out"));
            }
        });

        pendingConnects.put(ch, new NettyMicroPendingConnect(result, timeout));

        result.on(new FutureFinished() {
            @Override
            public void finished() throws Exception {
                pendingConnects.remove(ch);
            }
        });

        return result;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        public static SocketAddress DEFAULT_ADDRESS = new InetSocketAddress("127.0.0.1", 8100);
        public static final int DEFAULT_PARENT_THREADS = 2;
        public static final int DEFAULT_CHILD_THREADS = 10;
        public static final long DEFAULT_IDLE_TIMEOUT = 2000;
        public static final long DEFAULT_HEARTBEAT_TIMEOUT = 1000;
        public static final int MAX_FRAME_SIZE = Integer.MAX_VALUE;

        private UUID localId;
        private AsyncFramework async;
        private SerializerFramework serializer;
        private SocketAddress address = DEFAULT_ADDRESS;
        private int parentThreads = DEFAULT_PARENT_THREADS;
        private int childThreads = DEFAULT_CHILD_THREADS;
        private MicroRouter router;

        private long idleTimeout = DEFAULT_IDLE_TIMEOUT;
        private int maxFrameSize = MAX_FRAME_SIZE;
        private long heartbeatTimeout = DEFAULT_HEARTBEAT_TIMEOUT;

        public Builder localId(UUID localId) {
            this.localId = localId;
            return this;
        }

        public Builder router(MicroRouter router) {
            this.router = router;
            return this;
        }

        public Builder address(SocketAddress address) {
            if (address == null)
                throw new IllegalArgumentException("address: must not be null");

            this.address = address;
            return this;
        }

        public Builder parentThreads(int parentThreads) {
            if (parentThreads < 1)
                throw new IllegalArgumentException("parentThreads: must be a positive value");

            this.parentThreads = parentThreads;
            return this;
        }

        public Builder childThreads(int childThreads) {
            if (childThreads < 1)
                throw new IllegalArgumentException("childThreads: must be a positive value");

            this.childThreads = childThreads;
            return this;
        }

        public Builder async(AsyncFramework async) {
            if (async == null)
                throw new IllegalArgumentException("async framework must be non-null");

            this.async = async;
            return this;
        }

        public Builder serializer(SerializerFramework serializer) {
            if (serializer == null)
                throw new IllegalArgumentException("serializer must be non-null");

            this.serializer = serializer;
            return this;
        }

        public Builder idleTimeout(long idleTimeout) {
            if (idleTimeout <= 0)
                throw new IllegalArgumentException("idleTimeout: must be larger than zero");

            this.idleTimeout = idleTimeout;
            return this;
        }

        public Builder maxFrameSize(int maxFrameSize) {
            if (maxFrameSize < 1024)
                throw new IllegalArgumentException("maxFrameSize: must be at least 1024 bytes");

            this.maxFrameSize = maxFrameSize;
            return this;
        }

        public Builder heartbeatTimeout(long heartbeatTimeout) {
            if (heartbeatTimeout <= 0)
                throw new IllegalArgumentException("idleTimeout: must be larger than zero");

            this.heartbeatTimeout = heartbeatTimeout;
            return this;
        }

        public MicroNetwork build() {
            if (async == null)
                throw new IllegalArgumentException("async framework must be non-null");

            if (serializer == null)
                throw new IllegalArgumentException("message serializer must be non-null");

            if (router == null)
                throw new IllegalArgumentException("router must not be null");

            final UUID localId = this.localId == null ? UUID.randomUUID() : this.localId;

            final MicroSerializers s = new MicroSerializers(serializer);

            final Serializer<MicroMessage> messageSerializer = s.message();

            final EventLoopGroup parent = new EpollEventLoopGroup(parentThreads, new ThreadFactoryBuilder()
                    .setNameFormat("micro-server-parent-%d").build());

            final EventLoopGroup child = new EpollEventLoopGroup(childThreads, new ThreadFactoryBuilder()
                    .setNameFormat("micro-server-child-%d").build());

            final MicroThreadTimer timer = new MicroThreadTimer(child);

            timer.setName("micro-network-timer");
            timer.start();

            return new NettyMicroNetwork(async, messageSerializer, address, router, timer, parent, child, idleTimeout,
                    maxFrameSize, heartbeatTimeout, localId);
        }
    }
}
