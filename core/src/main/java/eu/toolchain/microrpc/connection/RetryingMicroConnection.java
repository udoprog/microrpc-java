package eu.toolchain.microrpc.connection;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureDone;
import eu.toolchain.async.FutureFinished;
import eu.toolchain.async.ResolvableFuture;
import eu.toolchain.microrpc.MicroConnection;
import eu.toolchain.microrpc.timer.MicroTimer;

@Slf4j
@ToString(of = { "connection", "retries", "backoff" })
public class RetryingMicroConnection extends AbstractOperationMicroConnection {
    private final AsyncFramework async;
    private final MicroConnection connection;
    private final int retries;
    private final long backoff;
    private final MicroTimer timer;
    private final AtomicInteger pendingRetries;
    private final int maxPendingRetries;

    /**
     * Maintain a set of pending retries, so they can be failed if the underlying connection fails.
     */
    private final Set<AsyncFuture<?>> pending = Collections
            .newSetFromMap(new ConcurrentHashMap<AsyncFuture<?>, Boolean>());

    public RetryingMicroConnection(AsyncFramework async, MicroConnection c, int retries, long backoff,
            MicroTimer timer, AtomicInteger pendingRetries, int maxPendingRetries) {
        c.retain();

        this.async = async;
        this.connection = c;
        this.retries = retries;
        this.backoff = backoff;
        this.timer = timer;
        this.pendingRetries = pendingRetries;
        this.maxPendingRetries = maxPendingRetries;

        c.closeFuture().on(new FutureFinished() {
            @Override
            public void finished() throws Exception {
                close();
            }
        });
    }

    @Override
    public AsyncFuture<Void> closeFuture() {
        return connection.closeFuture();
    }

    @Override
    protected void dealloc() {
        for (final AsyncFuture<?> future : pending)
            future.fail(new Exception("connection de-allocated"));

        pending.clear();

        connection.release();
        connection.close();
    }

    @Override
    public boolean isReady() {
        return connection.isReady();
    }

    @Override
    protected <R> AsyncFuture<R> execute(Operation<R> operation) {
        retain();

        return doExecute(operation).on(new FutureFinished() {
            @Override
            public void finished() throws Exception {
                release();
            }
        });
    }

    private <R> AsyncFuture<R> doExecute(Operation<R> operation) {
        if (pendingRetryLimit())
            return async.failed(new Exception("max pending retries"));

        final ResolvableFuture<R> future = async.future();

        execute(operation, future, 0, new ArrayList<Throwable>());

        pending.add(future);

        return future.on(new FutureFinished() {
            @Override
            public void finished() throws Exception {
                pending.remove(future);
                pendingRetries.decrementAndGet();
            }
        });
    }

    /**
     * Check if pending retry limit is reached.
     *
     * Be precise by spinning on the atomic update.
     *
     * @return
     */
    private boolean pendingRetryLimit() {
        while (true) {
            final int retry = pendingRetries.getAndIncrement();

            if (retry < maxPendingRetries)
                return false;

            if (!pendingRetries.compareAndSet(retry + 1, retry))
                continue;

            return true;
        }
    }

    private <R> void execute(final Operation<R> operation, final ResolvableFuture<R> future, final int attempt,
            final List<Throwable> causes) {
        operation.run(connection).on(new FutureDone<R>() {
            @Override
            public void failed(final Throwable cause) throws Exception {
                failure(operation, future, attempt, causes, cause);
            }

            @Override
            public void cancelled() throws Exception {
                failure(operation, future, attempt, causes, null);
            }

            @Override
            public void resolved(R result) throws Exception {
                future.resolve(result);
            }
        });
    }

    private <R> void failure(final Operation<R> operation, final ResolvableFuture<R> future, final int attempt,
            final List<Throwable> causes, final Throwable cause) {
        // accumulate all causes.
        if (cause != null)
            causes.add(cause);

        final int next = attempt + 1;

        if (next >= retries) {
            final Exception e = new Exception("max retries (" + retries + ") reached");

            for (Throwable c : causes)
                e.addSuppressed(c);

            future.fail(e);
            return;
        }

        long delay = (long) (backoff * Math.pow(2, attempt));

        if (log.isDebugEnabled())
            log.debug("retry #{} in {}ms {}", attempt, delay, connection);

        timer.schedule(delay, new MicroTimer.Task() {
            @Override
            public void run(MicroTimer.TaskSchedule t) throws Exception {
                execute(operation, future, next, causes);
            }
        });
    }
}