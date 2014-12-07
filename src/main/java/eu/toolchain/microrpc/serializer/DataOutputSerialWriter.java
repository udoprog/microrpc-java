package eu.toolchain.microrpc.serializer;

import java.io.IOException;

import lombok.RequiredArgsConstructor;

import com.google.common.io.ByteArrayDataOutput;

import eu.toolchain.serializer.io.AbstractSerialWriter;

@RequiredArgsConstructor
public class DataOutputSerialWriter extends AbstractSerialWriter {
    private final ByteArrayDataOutput output;

    @Override
    public void write(int b) throws IOException {
        output.writeByte(b);
    }

    @Override
    public void write(byte[] bytes) throws IOException {
        output.write(bytes);
    }

    public byte[] toByteArray() {
        return output.toByteArray();
    }
}