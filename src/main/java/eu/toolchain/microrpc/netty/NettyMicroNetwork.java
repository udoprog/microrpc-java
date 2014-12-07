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

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureFinished;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.microrpc.MicroConnection;
import eu.toolchain.microrpc.MicroConnectionProvider;
import eu.toolchain.microrpc.MicroNetwork;
import eu.toolchain.microrpc.MicroNetworkState;
import eu.toolchain.microrpc.MicroReceiver;
import eu.toolchain.microrpc.messages.MicroMessage;
import eu.toolchain.microrpc.messages.MicroMetadata;
import eu.toolchain.microrpc.timer.MicroThreadTimer;
import eu.toolchain.microrpc.timer.Task;
import eu.toolchain.microrpc.timer.TaskSchedule;
import eu.toolchain.serializer.Serializer;

@Slf4j
@ToString(of = { "address", "parentThreads", "childThreads", "idleTimeout", "keepAliveTime", "maxFrameSize" })
public class NettyMicroNetwork implements MicroNetwork {
    private final AsyncFramework async;
    private final Serializer<MicroMessage> messageSerializer;
    private final MicroReceiver receiver;
    private final MicroConnectionProvider connectionProvider;
    private final UUID localId;
    private final SocketAddress address;
    private final int parentThreads;
    private final int childThreads;
    private final long idleTimeout;
    private final long keepAliveTime;
    private final int maxFrameSize;

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

    public NettyMicroNetwork(AsyncFramework async, Serializer<MicroMessage> messageSerializer, MicroReceiver receiver,
            MicroConnectionProvider connectionProvider, UUID localId, SocketAddress address, int parentThreads,
            int childThreads, long idleTimeout, long keepAliveTime, int maxFrameSize) {
        this.async = async;
        this.messageSerializer = messageSerializer;
        this.receiver = receiver;
        this.connectionProvider = connectionProvider;
        this.localId = localId;
        this.address = address;
        this.parentThreads = parentThreads;
        this.childThreads = childThreads;
        this.idleTimeout = idleTimeout;
        this.keepAliveTime = keepAliveTime;
        this.maxFrameSize = maxFrameSize;

        this.incoming = initializer(handler(MicroConnection.Direction.INCOMING));
        this.outgoing = initializer(handler(MicroConnection.Direction.OUTGOING));

        this.messageDecoder = new NettyMicroMessageDecoder(messageSerializer);
    }

    @Override
    public long idleTimeout() {
        return idleTimeout;
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

                receiver.receive(c, message);
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
        final NettyMicroConnection c = new NettyMicroConnection(async, connectionProvider, d, m.getRemoteId(), ch);

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
    public AsyncFuture<MicroConnection> connect(final MicroNetworkState state, final String host, final int port) {
        final NettyMicroNetworkServerState s = (NettyMicroNetworkServerState) state;

        final InetSocketAddress remoteAddress = new InetSocketAddress(host, port);
        final Bootstrap b = new Bootstrap();

        b.channel(EpollSocketChannel.class);
        b.group(s.childGroup());
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

        final TaskSchedule timeout = s.timer().schedule(idleTimeout, new Task() {
            @Override
            public void run(TaskSchedule t) throws Exception {
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

    /**
     * Sets up bind state.
     *
     * @return
     * @throws InterruptedException
     */
    @Override
    public MicroNetworkState setup() throws InterruptedException {
        final ServerBootstrap server = new ServerBootstrap();

        final EventLoopGroup parent = new EpollEventLoopGroup(parentThreads, new ThreadFactoryBuilder().setNameFormat(
                "micro-server-parent-%d").build());

        final EventLoopGroup child = new EpollEventLoopGroup(childThreads, new ThreadFactoryBuilder().setNameFormat(
                "micro-server-child-%d").build());

        server.channel(EpollServerSocketChannel.class).group(parent, child).childHandler(incoming);
        final Channel serverChannel = server.bind(address).sync().channel();

        final MicroThreadTimer timer = new MicroThreadTimer(child);

        timer.setName("micro-server-timeout");
        timer.start();

        return new NettyMicroNetworkServerState(serverChannel, parent, child, timer, idleTimeout, keepAliveTime);
    }
}
