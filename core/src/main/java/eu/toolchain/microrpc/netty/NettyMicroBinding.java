package eu.toolchain.microrpc.netty;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import lombok.Data;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.microrpc.MicroBinding;

@Data
public class NettyMicroBinding implements MicroBinding {
    private final AsyncFramework async;
    private final Channel channel;

    @Override
    public AsyncFuture<Void> close() {
        final ResolvableFuture<Void> future = async.future();

        channel.close().addListener(new ChannelFutureListener() {
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
}