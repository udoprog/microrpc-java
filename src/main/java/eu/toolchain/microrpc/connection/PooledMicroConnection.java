package eu.toolchain.microrpc.connection;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import lombok.RequiredArgsConstructor;
import lombok.ToString;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureFinished;
import eu.toolchain.microrpc.MicroConnection;

/**
 * A pool of multiple connections.
 *
 * Can be used to increase throughput between two services.
 */
@RequiredArgsConstructor
@ToString(exclude = { "random" })
public class PooledMicroConnection extends AbstractOperationMicroConnection {
    private final AsyncFramework async;
    private final List<MicroConnection> connections;
    private final Random random;
    private final AsyncFuture<Void> close;

    public PooledMicroConnection(AsyncFramework async, List<MicroConnection> connections, Random random) {
        this.async = async;
        this.connections = connections;
        this.random = random;
        this.close = buildCloseFuture(connections);
    }

    @Override
    public AsyncFuture<Void> closeFuture() {
        return close;
    }

    @Override
    protected void dealloc() {
        for (final MicroConnection c : connections)
            c.close();
    }

    private AsyncFuture<Void> buildCloseFuture(List<MicroConnection> connections) {
        final List<AsyncFuture<Void>> futures = new ArrayList<>();

        for (final MicroConnection c : connections)
            futures.add(c.closeFuture());

        return async.collectAndDiscard(futures).on(new FutureFinished() {
            @Override
            public void finished() throws Exception {
                close();
            }
        });
    }

    @Override
    protected <R> AsyncFuture<R> execute(Operation<R> operation) {
        final MicroConnection c = pick();

        if (c == null)
            return async.failed(new IllegalStateException("no connected connections available"));

        try {
            return operation.run(c);
        } finally {
            c.release();
        }
    }

    private MicroConnection pick() {
        final int size = connections.size();

        int start = random.nextInt(connections.size());

        for (int i = 0; i < size; i++) {
            final MicroConnection candidate = connections.get((i + start) % size);

            // make sure it does not get de-allocated while we want to operate in ot.
            candidate.retain();

            if (candidate.isReady())
                return candidate;

            candidate.release();
        }

        return null;
    }

    @Override
    public boolean isReady() {
        for (final MicroConnection c : connections) {
            if (c.isReady())
                return true;
        }

        return false;
    }
}