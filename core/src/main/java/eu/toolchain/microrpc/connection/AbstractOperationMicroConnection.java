package eu.toolchain.microrpc.connection;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.microrpc.MicroConnection;
import eu.toolchain.microrpc.messages.MicroMessage;
import eu.toolchain.serializer.Serializer;

public abstract class AbstractOperationMicroConnection extends AbstractMicroConnection {
    @Override
    public final <R> AsyncFuture<R> request(final String endpoint, final Serializer<R> result) {
        return execute(new Operation<R>() {
            @Override
            public AsyncFuture<R> run(MicroConnection c) {
                return c.request(endpoint, result);
            }
        });
    }

    @Override
    public final <R> AsyncFuture<R> request(final String endpoint, final Serializer<R> result, final Integer version) {
        return execute(new Operation<R>() {
            @Override
            public AsyncFuture<R> run(MicroConnection c) {
                return c.request(endpoint, result, version);
            }
        });
    }

    @Override
    public final <B, R> AsyncFuture<R> request(final String endpoint, final Serializer<R> result,
            final Integer version, final B bodyValue, final Serializer<B> body) {
        return execute(new Operation<R>() {
            @Override
            public AsyncFuture<R> run(MicroConnection c) {
                return c.request(endpoint, result, version, bodyValue, body);
            }
        });
    }

    @Override
    public final AsyncFuture<Void> send(final MicroMessage message) {
        return execute(new Operation<Void>() {
            @Override
            public AsyncFuture<Void> run(MicroConnection c) {
                return c.send(message);
            }
        });
    }

    /**
     * Implement for the operation to execute.
     *
     * @param operation Operation to run.
     * @return A future for the specific operation.
     */
    abstract protected <R> AsyncFuture<R> execute(Operation<R> operation);

    protected static interface Operation<R> {
        public AsyncFuture<R> run(MicroConnection c);
    }
}
