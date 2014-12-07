package eu.toolchain.microrpc;

import java.util.HashMap;
import java.util.Map;

import eu.toolchain.serializer.Serializer;

public class MicroRouterImpl implements MicroRouter {
    private final Map<Integer, MicroVersionMapping> versions = new HashMap<>();

    @Override
    public void listen(MicroListenerSetup setup) {
        listen(null, setup);
    }

    @Override
    public void listen(final Integer version, MicroListenerSetup setup) {
        final MicroVersionMapping m = new MicroVersionMapping(version);

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
    public MicroVersionMapping resolve(Integer version) {
        return versions.get(version);
    }
}
