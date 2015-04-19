package eu.toolchain.microrpc.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

import lombok.RequiredArgsConstructor;
import eu.toolchain.microrpc.messages.MicroMessage;
import eu.toolchain.microrpc.serializer.ByteBufSerialReader;
import eu.toolchain.serializer.Serializer;

@RequiredArgsConstructor
public final class NettyMicroMessageDecoder extends MessageToMessageDecoder<ByteBuf> {
    private final Serializer<MicroMessage> serializer;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf frame, List<Object> out) throws Exception {
        out.add(serializer.deserialize(new ByteBufSerialReader(frame)));
    }

    @Override
    public boolean isSharable() {
        return true;
    }
}