package eu.toolchain.microrpc.connection;

import java.util.concurrent.atomic.AtomicReference;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;
import eu.toolchain.async.FutureFinished;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.microrpc.MicroConnection;
import eu.toolchain.microrpc.messages.MicroMessage;
import eu.toolchain.microrpc.timer.MicroTimer;
import eu.toolchain.serializer.Serializer;

@Slf4j
@ToString(of = { "connection", "reconnectTimeout" })
public class ReconnectingMicroConnection extends AbstractMicroConnection {
    private final AsyncFramework async;
    private final AtomicReference<MicroConnection> connection;

    /**
     * If sets, gives this connection the means to re-establish its connection.
     */
    private final Establish establish;

    private final MicroTimer timer;

    /**
     * How long to wait, before re-establishing the connection.
     */
    private final long reconnectTimeout;

    /**
     * Has this connection been closed?
     */
    private final ResolvableFuture<Void> close;

    public ReconnectingMicroConnection(AsyncFramework async, MicroConnection c, Establish establish, MicroTimer timer,
            long reconnectTimeout, ResolvableFuture<Void> close) {
        c.retain();

        this.async = async;
        this.connection = new AtomicReference<>(c);
        this.establish = establish;
        this.timer = timer;
        this.reconnectTimeout = reconnectTimeout;
        this.close = close;

        reconnectOnClose(c);
    }

    @Override
    public <R> AsyncFuture<R> request(final String endpoint, final Serializer<R> result) {
        return execute(new Operation<R>() {
            @Override
            public AsyncFuture<R> run(MicroConnection c) {
                return c.request(endpoint, result);
            }
        });
    }

    @Override
    public <R> AsyncFuture<R> request(final String endpoint, final Serializer<R> result, final Integer version) {
        return execute(new Operation<R>() {
            @Override
            public AsyncFuture<R> run(MicroConnection c) {
                return c.request(endpoint, result, version);
            }
        });
    }

    @Override
    public <B, R> AsyncFuture<R> request(final String endpoint, final Serializer<R> result, final Integer version,
            final B bodyValue, final Serializer<B> body) {
        return execute(new Operation<R>() {
            @Override
            public AsyncFuture<R> run(MicroConnection c) {
                return c.request(endpoint, result, version, bodyValue, body);
            }
        });
    }

    @Override
    public AsyncFuture<Void> send(final MicroMessage message) {
        return execute(new Operation<Void>() {
            @Override
            public AsyncFuture<Void> run(MicroConnection c) {
                return c.send(message);
            }
        });
    }

    @Override
    public AsyncFuture<Void> closeFuture() {
        return close;
    }

    /**
     * de-allocate underlying connection.
     */
    @Override
    protected void dealloc() {
        final MicroConnection c = connection.getAndSet(null);

        if (c == null)
            return;

        c.release();

        c.closeFuture().on(new FutureDone<Void>() {
            @Override
            public void failed(Throwable e) throws Exception {
                close.fail(e);
            }

            @Override
            public void resolved(Void result) throws Exception {
                close.resolve(result);
            }

            @Override
            public void cancelled() throws Exception {
                close.cancel();
            }
        });

        // depend on the last available connection to close.
        c.close();
    }

    @Override
    public boolean isReady() {
        final MicroConnection c = connection.get();

        if (c == null)
            return false;

        return c.isReady();
    }

    private void reconnectOnClose(final MicroConnection c) {
        c.closeFuture().on(new FutureFinished() {
            @Override
            public void finished() throws Exception {
                reconnect(c);
            }
        });
    }

    private void reconnect(MicroConnection c) {
        // channel was explicitly closed.
        if (isCloseRequested() || c.isCloseRequested()) {
            log.info("reconnect() stop {}", this);
            close();
            return;
        }

        establish(c);
    }

    private void establish(final MicroConnection c) {
        establish.establish().on(new FutureDone<MicroConnection>() {
            @Override
            public void failed(Throwable e) throws Exception {
                log.error("connection lost, retrying in {}ms: {}", reconnectTimeout, e.getMessage());

                timer.schedule(reconnectTimeout, new MicroTimer.Task() {
                    @Override
                    public void run(MicroTimer.TaskSchedule t) throws Exception {
                        reconnect(c);
                    }
                });
            }

            @Override
            public void cancelled() throws Exception {
                log.error("connect cancelled, retrying in {}ms: {}", reconnectTimeout);

                timer.schedule(reconnectTimeout, new MicroTimer.Task() {
                    @Override
                    public void run(MicroTimer.TaskSchedule t) throws Exception {
                        reconnect(c);
                    }
                });
            }

            @Override
            public void resolved(MicroConnection c) throws Exception {
                final MicroConnection old = connection.getAndSet(c);
                reconnectOnClose(c);
                old.release();
            }
        });
    }

    private <R> AsyncFuture<R> execute(Operation<R> operation) {
        final MicroConnection c = connection.get();

        if (c == null)
            return async.failed(new IllegalStateException("connection not available"));

        return operation.run(c);
    }

    public static interface Establish {
        AsyncFuture<MicroConnection> establish();
    }

    private static interface Operation<R> {
        AsyncFuture<R> run(MicroConnection c);
    }
}
