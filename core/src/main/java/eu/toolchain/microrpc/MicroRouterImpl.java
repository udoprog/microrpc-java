package eu.toolchain.microrpc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import lombok.RequiredArgsConstructor;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.microrpc.messages.MicroRequestResponse;
import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.io.ByteArraySerialReader;

public class MicroRouterImpl implements MicroRouter {
    private final Map<Integer, MicroRouter.Mapping> versions = new HashMap<>();

    @Override
    public void listen(MicroListenerSetup setup) {
        listen(null, setup);
    }

    @Override
    public void listen(final Integer version, MicroListenerSetup setup) {
        final MicroRouter.Mapping m = new MappingImpl(version);

        // hook to populate endpoints.
        final MicroEndPointSetup hook = new MicroEndPointSetup() {
            @Override
            public <R> void on(String endpoint, Serializer<R> response, MicroEndPoint<Void, R> handle) {
                on(endpoint, null, response, handle);
            }

            @Override
            public <Q, R> void on(String name, Serializer<Q> query, Serializer<R> result, MicroEndPoint<Q, R> endpoint) {
                if (result == null)
                    throw new IllegalArgumentException("response serializer must be non-null");

                m.register(name, query, result, endpoint);
            }
        };

        setup.setup(hook);

        if (this.versions.put(version, m) != null)
            throw new IllegalArgumentException("A mapping already registered for version: " + version);
    }

    @Override
    public MicroRouter.Mapping resolve(Integer version) {
        return versions.get(version);
    }

    @RequiredArgsConstructor
    public static final class MappingImpl implements MicroRouter.Mapping {
        private final Integer version;
        private final Map<String, MicroRouter.EndpointHelper<?, ?>> endpoints = new HashMap<>();

        public <Q, R> void register(String name, Serializer<Q> query, Serializer<R> result, MicroEndPoint<Q, R> handle) {
            if (endpoints.put(name, new EndpointHelperImpl<Q, R>(this, query, result, handle)) != null)
                throw new IllegalArgumentException("A mapping for endpoint " + name + " already exists");
        }

        public MicroRequestResponse result(UUID requestId, byte[] body) throws IOException {
            return new MicroRequestResponse(version, requestId, body);
        }

        public MicroRouter.EndpointHelper<?, ?> endpoint(String endpoint) {
            return endpoints.get(endpoint);
        }
    }

    @RequiredArgsConstructor
    public static final class EndpointHelperImpl<Q, R> implements MicroRouter.EndpointHelper<Q, R> {
        private final MicroRouter.Mapping version;
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
}