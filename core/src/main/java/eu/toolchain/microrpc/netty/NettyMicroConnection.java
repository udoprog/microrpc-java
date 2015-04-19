package eu.toolchain.microrpc.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;

import java.io.IOException;
import java.util.UUID;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;

import com.google.common.io.ByteStreams;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureFinished;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.async.Transform;
import eu.toolchain.microrpc.MicroNetwork;
import eu.toolchain.microrpc.MicroNetwork.PendingRequestContext;
import eu.toolchain.microrpc.connection.AbstractMicroConnection;
import eu.toolchain.microrpc.messages.MicroErrorMessage;
import eu.toolchain.microrpc.messages.MicroMessage;
import eu.toolchain.microrpc.messages.MicroRequest;
import eu.toolchain.microrpc.messages.MicroRequestResponse;
import eu.toolchain.microrpc.serializer.DataOutputSerialWriter;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.io.ByteArraySerialReader;

/**
 * A single, one-to-one connection.
 */
@Slf4j
@ToString(of = { "channel", "direction", "peerId" })
final class NettyMicroConnection extends AbstractMicroConnection {
    private static final byte[] EMPTY_BODY = new byte[0];

    private final AsyncFramework async;
    private final MicroNetwork network;
    private final Direction direction;
    private final UUID peerId;
    private final Channel channel;

    /**
     * Future that will be invoked once this has been closed.
     */
    private final ResolvableFuture<Void> close;

    public NettyMicroConnection(AsyncFramework async, MicroNetwork network, Direction direction, UUID peerId,
            Channel channel) {
        this.async = async;
        this.network = network;
        this.direction = direction;
        this.peerId = peerId;
        this.channel = channel;
        this.close = async.future();

        // listen for close, regardless of reason.
        this.channel.closeFuture().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                close.resolve(null);
            }
        });
    }

    @Override
    protected void dealloc() {
        channel.close().addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess())
                    close.fail(future.cause());
            }
        });
    }

    @Override
    public <R> AsyncFuture<R> request(String endpoint, Serializer<R> response) {
        return request(endpoint, response, null);
    }

    @Override
    public <R> AsyncFuture<R> request(String endpoint, Serializer<R> response, Integer version) {
        return request(endpoint, response, version, null, null);
    }

    @Override
    public <B, R> AsyncFuture<R> request(String endpoint, Serializer<R> response, Integer version, B b,
            Serializer<B> body) {
        // retain instance.
        retain();

        return doRequest(endpoint, response, version, b, body).on(new FutureFinished() {
            @Override
            public void finished() throws Exception {
                // release instance when request has finished.
                release();
            }
        });
    }

    private <R, B> AsyncFuture<R> doRequest(String endpoint, Serializer<R> responseSerializer, Integer version, B body,
            Serializer<B> bodySerializer) {
        final MicroRequest request;

        try {
            request = buildRequest(version, endpoint, body, bodySerializer);
        } catch (IOException e) {
            return async.failed(e);
        }

        final AsyncFuture<R> future = receive(responseSerializer, request.getId());
        send(request);
        return future;
    }

    @Override
    public AsyncFuture<Void> send(MicroMessage message) {
        if (log.isDebugEnabled())
            log.debug("=> {} {}", this, message);

        final ResolvableFuture<Void> result = async.future();

        channel.writeAndFlush(message).addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (!future.isSuccess()) {
                    result.fail(future.cause());
                    return;
                }

                result.resolve(null);
            }
        });

        return result;
    }

    @Override
    public AsyncFuture<Void> closeFuture() {
        return close;
    }

    @Override
    public boolean isReady() {
        return !close.isDone();
    }

    public Direction direction() {
        return direction;
    }

    public UUID peerId() {
        return peerId;
    }

    private <Q> MicroRequest buildRequest(Integer version, String endpoint, Q query, Serializer<Q> querySerializer)
            throws IOException {
        final byte[] body = buildBody(query, querySerializer);
        final UUID id = UUID.randomUUID();
        return new MicroRequest(version, id, endpoint, body);
    }

    private <Q> byte[] buildBody(Q query, Serializer<Q> querySerializer) throws IOException {
        if (query == null)
            return EMPTY_BODY;

        final DataOutputSerialWriter out = new DataOutputSerialWriter(ByteStreams.newDataOutput());
        querySerializer.serialize(out, query);
        return out.toByteArray();
    }

    private <R> AsyncFuture<R> receive(final Serializer<R> serializer, final UUID requestId) {
        final ResolvableFuture<MicroMessage> future = async.future();

        final PendingRequestContext context = network.createPending(this, requestId, future);

        final Transform<MicroMessage, R> transform = new Transform<MicroMessage, R>() {
            @Override
            public R transform(MicroMessage result) throws Exception {
                if (result instanceof MicroErrorMessage)
                    throw new Exception(((MicroErrorMessage) result).getMessage());

                if (!(result instanceof MicroRequestResponse))
                    throw new Exception("Unexpected response type: " + result.getClass());

                final MicroRequestResponse response = (MicroRequestResponse) result;
                return serializer.deserialize(new ByteArraySerialReader(response.getBody()));
            }
        };

        // when future is done (for any reason), remove pending request.
        future.on(new FutureFinished() {
            @Override
            public void finished() throws Exception {
                context.stop();
            }
        });

        return future.transform(transform);
    }
}