package eu.toolchain.microrpc;

import java.io.IOException;
import java.util.UUID;

import lombok.RequiredArgsConstructor;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.microrpc.messages.MicroRequestResponse;
import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.io.ByteArraySerialReader;

@RequiredArgsConstructor
public final class MicroEndpointMapping<Q, R> {
    private final MicroVersionMapping version;
    private final Serializer<Q> query;
    private final Serializer<R> response;
    private final MicroEndPoint<Q, R> endpoint;

    @SuppressWarnings("unchecked")
    public AsyncFuture<Object> query(Object query) throws Exception {
        return (AsyncFuture<Object>) endpoint.query((Q) query);
    }

    public Object deserialize(SerialReader buffer) throws IOException {
        return query.deserialize(buffer);
    }

    @SuppressWarnings("unchecked")
    public void serialize(SerialWriter buffer, Object value) throws IOException {
        response.serialize(buffer, (R) value);
    }

    public boolean expectsQuery() {
        return query != null;
    }

    public Object deserialize(byte[] body) throws IOException {
        if (!expectsQuery()) {
            if (body.length != 0)
                throw new IllegalStateException("got body content, but endpoint does not take any");

            return null;
        }

        return deserialize(new ByteArraySerialReader(body));
    }

    public MicroRequestResponse result(UUID id, byte[] body) throws IOException {
        return version.result(id, body);
    }
}
