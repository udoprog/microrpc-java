package eu.toolchain.microrpc.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;
import lombok.RequiredArgsConstructor;
import eu.toolchain.microrpc.messages.MicroMessage;
import eu.toolchain.microrpc.serializer.ByteBufSerialWriter;
import eu.toolchain.serializer.Serializer;

@RequiredArgsConstructor
public final class NettyMicroMessageEncoder extends MessageToByteEncoder<MicroMessage> {
    private final Serializer<MicroMessage> serializer;

    @Override
    protected void encode(ChannelHandlerContext ctx, MicroMessage message, ByteBuf out) throws Exception {
        serializer.serialize(new ByteBufSerialWriter(out), message);
    }
}