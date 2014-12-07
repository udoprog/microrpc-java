package eu.toolchain.microrpc.netty;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.UUID;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.microrpc.MicroConnectionProvider;
import eu.toolchain.microrpc.MicroNetwork;
import eu.toolchain.microrpc.MicroNetworkConfig;
import eu.toolchain.microrpc.MicroReceiver;
import eu.toolchain.microrpc.messages.MicroMessage;
import eu.toolchain.serializer.Serializer;

public class NettyMicroNetworkConfig implements MicroNetworkConfig {
    private final SocketAddress address;
    private final int parentThreads;
    private final int childThreads;
    private final long idleTimeout;
    private final long keepAliveTime;
    private final int maxFrameSize;

    private NettyMicroNetworkConfig(SocketAddress address, int parentThreads, int childThreads, long idleTimeout,
            int maxFrameSize) {
        this.address = address;
        this.parentThreads = parentThreads;
        this.childThreads = childThreads;
        this.idleTimeout = idleTimeout;
        this.keepAliveTime = idleTimeout / 2;
        this.maxFrameSize = maxFrameSize;
    }

    @Override
    public MicroNetwork setup(AsyncFramework async, Serializer<MicroMessage> message, MicroReceiver receiver,
            MicroConnectionProvider connectionProvider, UUID localId) {
        return new NettyMicroNetwork(async, message, receiver, connectionProvider, localId, address, parentThreads,
                childThreads, idleTimeout, keepAliveTime, maxFrameSize);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        public static SocketAddress DEFAULT_ADDRESS = new InetSocketAddress("127.0.0.1", 8100);
        public static final int DEFAULT_PARENT_THREADS = 2;
        public static final int DEFAULT_CHILD_THREADS = 10;
        public static final long DEFAULT_KEEP_ALIVE_TIME = 2000;
        public static final int MAX_FRAME_SIZE = Integer.MAX_VALUE;

        private SocketAddress address = DEFAULT_ADDRESS;
        private int parentThreads = DEFAULT_PARENT_THREADS;
        private int childThreads = DEFAULT_CHILD_THREADS;
        private long keepAliveTime = DEFAULT_KEEP_ALIVE_TIME;
        private int maxFrameSize = MAX_FRAME_SIZE;

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

        public Builder keepAliveTime(long keepAliveTime) {
            if (keepAliveTime < 1)
                throw new IllegalArgumentException("keepAliveTime: must be a positive value");

            this.keepAliveTime = keepAliveTime;
            return this;
        }

        public Builder maxFrameSize(int maxFrameSize) {
            if (maxFrameSize < 1024)
                throw new IllegalArgumentException("maxFrameSize: must be at least 1024 bytes");

            this.maxFrameSize = maxFrameSize;
            return this;
        }

        public NettyMicroNetworkConfig build() {
            if (address == null)
                throw new IllegalArgumentException("address: must be configured");

            return new NettyMicroNetworkConfig(address, parentThreads, childThreads, keepAliveTime, maxFrameSize);
        }
    }
}
