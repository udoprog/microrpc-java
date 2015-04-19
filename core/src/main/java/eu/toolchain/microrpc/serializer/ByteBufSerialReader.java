package eu.toolchain.microrpc.serializer;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import lombok.RequiredArgsConstructor;
import eu.toolchain.serializer.io.AbstractSerialReader;

@RequiredArgsConstructor
public class ByteBufSerialReader extends AbstractSerialReader {
    private final ByteBuf buffer;

    @Override
    public byte read() throws IOException {
        return buffer.readByte();
    }

    @Override
    public void read(byte[] b) throws IOException {
        buffer.readBytes(b);
    }

    @Override
    public void skip(int length) throws IOException {
        buffer.skipBytes(length);
    }
}