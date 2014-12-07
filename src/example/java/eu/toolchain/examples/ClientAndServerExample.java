package eu.toolchain.examples;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.TinyAsync;
import eu.toolchain.microrpc.MicroConnection;
import eu.toolchain.microrpc.MicroEndPoint;
import eu.toolchain.microrpc.MicroEndPointSetup;
import eu.toolchain.microrpc.MicroListenerSetup;
import eu.toolchain.microrpc.MicroNetworkConfig;
import eu.toolchain.microrpc.MicroRouter;
import eu.toolchain.microrpc.MicroRouterImpl;
import eu.toolchain.microrpc.MicroServer;
import eu.toolchain.microrpc.netty.NettyMicroNetworkConfig;
import eu.toolchain.serializer.SerializerFramework;
import eu.toolchain.serializer.TinySerializer;

public class ClientAndServerExample {
    public static MicroServer buildServer(final SerializerFramework s, final AsyncFramework async) {
        final MicroServer.Builder builder = MicroServer.builder();

        builder.serializer(s);
        builder.async(async);

        final MicroNetworkConfig networkConfig = NettyMicroNetworkConfig.builder().build();

        builder.networkConfig(networkConfig);

        final MicroRouter router = new MicroRouterImpl();

        // listen on version 0.
        router.listen(0, new MicroListenerSetup() {
            @Override
            public void setup(MicroEndPointSetup endpoint) {
                endpoint.on("test", s.integer(), s.string(), new MicroEndPoint<Integer, String>() {
                    @Override
                    public AsyncFuture<String> query(Integer query) throws Exception {
                        return async.resolved("response #" + query);
                    }
                });
            }
        });

        builder.router(router);

        return builder.build();
    }

    public static void main(String argv[]) throws Exception {
        final TinySerializer s = TinySerializer.builder().build();
        final TinyAsync async = TinyAsync.builder().build();

        try (final MicroServer server = buildServer(s, async)) {
            server.start();

            // multiplex requests over 20 connections
            try (final MicroConnection c = server.connect(new URI("rpc://localhost:8100"), 20).get()) {
                // send 1000 requests and receive results.
                final List<AsyncFuture<String>> requests = new ArrayList<>();

                for (int i = 0; i < 1000; i++)
                    requests.add(c.request("test", s.string(), 0, i, s.integer()));

                // collect all results using the default collector.
                for (final String result : async.collect(requests).get())
                    System.out.println("result: " + result);

                // endpoint "test" of version 1 does not exist.
                try {
                    c.request("test", s.string(), 1).get();
                } catch (ExecutionException e) {
                    System.out.println("failed request: " + e.getCause().getMessage());
                    e.getCause().printStackTrace(System.out);
                }
            }
        }
    }
}
