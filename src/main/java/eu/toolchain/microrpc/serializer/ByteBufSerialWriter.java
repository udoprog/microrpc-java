package eu.toolchain.microrpc.serializer;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import lombok.RequiredArgsConstructor;
import eu.toolchain.serializer.io.AbstractSerialWriter;

@RequiredArgsConstructor
public class ByteBufSerialWriter extends AbstractSerialWriter {
    private final ByteBuf buffer;

    @Override
    public void write(int b) throws IOException {
        buffer.writeByte(b);
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        buffer.writeBytes(bytes);
    }

    public ByteBuf toByteBuf() {
        return buffer.duplicate();
    }
}