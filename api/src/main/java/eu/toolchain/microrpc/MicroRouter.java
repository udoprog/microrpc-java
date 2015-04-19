package eu.toolchain.microrpc;

import java.io.IOException;
import java.util.UUID;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.microrpc.messages.MicroMessage;
import eu.toolchain.microrpc.messages.MicroRequestResponse;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;

public interface MicroRouter {
    public void listen(MicroListenerSetup setup);

    public void listen(final Integer version, MicroListenerSetup setup);

    public Mapping resolve(Integer version);

    public interface Mapping {
        public <Q, R> void register(String name, Serializer<Q> query, Serializer<R> result, MicroEndPoint<Q, R> handle);

        public MicroRequestResponse result(UUID id, byte[] body) throws IOException;

        public EndpointHelper<?, ?> endpoint(String endpoint);
    }

    public interface EndpointHelper<Q, R> {
        public Object deserialize(byte[] body) throws IOException;

        public void serialize(SerialWriter out, Object result) throws IOException;

        public MicroMessage result(UUID id, byte[] byteArray) throws IOException;

        public AsyncFuture<Object> query(Object query) throws Exception;
    }
}