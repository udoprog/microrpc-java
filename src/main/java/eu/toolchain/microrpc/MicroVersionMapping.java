package eu.toolchain.microrpc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import lombok.RequiredArgsConstructor;
import eu.toolchain.microrpc.messages.MicroRequestResponse;
import eu.toolchain.serializer.Serializer;

@RequiredArgsConstructor
public final class MicroVersionMapping {
    private final Integer version;
    private final Map<String, MicroEndpointMapping<?, ?>> endpoints = new HashMap<>();

    public <Q, R> void register(String name, Serializer<Q> query, Serializer<R> result, MicroEndPoint<Q, R> handle) {
        if (endpoints.put(name, new MicroEndpointMapping<Q, R>(this, query, result, handle)) != null)
            throw new IllegalArgumentException("A mapping for endpoint " + name + " already exists");
    }

    public MicroRequestResponse result(UUID requestId, byte[] body) throws IOException {
        return new MicroRequestResponse(version, requestId, body);
    }

    public MicroEndpointMapping<?, ?> endpoint(String endpoint) {
        return endpoints.get(endpoint);
    }
}
