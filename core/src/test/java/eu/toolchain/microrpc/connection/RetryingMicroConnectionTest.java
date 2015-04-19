package eu.toolchain.microrpc.connection;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.FutureFinished;
import eu.toolchain.microrpc.MicroConnection;
import eu.toolchain.microrpc.timer.MicroTimer;

@SuppressWarnings("unchecked")
public class RetryingMicroConnectionTest {
    private static final Object RESULT = new Object();
    private static final Exception CAUSE = new Exception("cause");

    private final int RETRIES = 3;
    private final long BACKOFF = 1000;
    private final int MAX_PENDING_RETRIES = 10;

    private AsyncFramework async;
    private MicroTimer timer;
    private AtomicInteger pendingRetries;
    private AsyncFuture<Void> closeFuture;
    private MicroConnection delegate;
    private RetryingMicroConnection c;
    private FutureFinished closed;

    private AsyncFuture<Object> failed;
    private AsyncFuture<Object> resolved;

    @Before
    public void setup() {
        async = Mockito.mock(AsyncFramework.class);
        timer = Mockito.mock(MicroTimer.class);
        pendingRetries = Mockito.mock(AtomicInteger.class);

        closeFuture = Mockito.mock(AsyncFuture.class);

        this.delegate = Mockito.mock(MicroConnection.class);
        Mockito.when(delegate.closeFuture()).thenReturn(closeFuture);

        this.c = new RetryingMicroConnection(async, delegate, RETRIES, BACKOFF, timer, pendingRetries,
                MAX_PENDING_RETRIES);

        final ArgumentCaptor<FutureFinished> finished = ArgumentCaptor.forClass(FutureFinished.class);
        Mockito.verify(closeFuture).on(finished.capture());

        this.closed = finished.getValue();

        failed = Mockito.mock(AsyncFuture.class);
        resolved = Mockito.mock(AsyncFuture.class);
    }

    /**
     * Emulate a series of failed, into successful requests.
     */
    private AsyncFuture<Object> testSequence(AsyncFuture<Object> first, AsyncFuture<Object>... rest) throws Exception {
        Assert.assertEquals(1, c.refCount());

        Mockito.when(delegate.request(null, null)).thenReturn(first, rest);

        // request type doesn't matter, internally they all use the same methods.
        AsyncFuture<Object> result = c.request(null, null);

        for (int i = 0; i < rest.length; i++) {
            Assert.assertEquals(2, c.refCount());
            assertAndRunTask(result, i);
        }

        Assert.assertEquals(1, c.refCount());
        return result;
    }

    /**
     * Assert that a task has been requested with the correct delay, and emulate its run.
     */
    private void assertAndRunTask(AsyncFuture<Object> result, int i) throws Exception {
        final ArgumentCaptor<MicroTimer.Task> task = ArgumentCaptor.forClass(MicroTimer.Task.class);
        Mockito.verify(timer).schedule(Mockito.eq((long) (BACKOFF * Math.pow(2, i))), task.capture());
        Assert.assertFalse(result.isDone());
        task.getValue().run(Mockito.mock(MicroTimer.TaskSchedule.class));
    }

    @Ignore
    @Test
    public void testSuccessful() throws Exception {
        final AsyncFuture<Object> result = testSequence(resolved);
        Assert.assertTrue(result.isDone());
        Assert.assertEquals(RESULT, result.getNow());
    }

    @Ignore
    @Test
    public void testFailThenSucceed() throws Exception {
        final AsyncFuture<Object> result = testSequence(failed, failed, resolved);
        Assert.assertTrue(result.isDone());
        Assert.assertEquals(RESULT, result.getNow());
    }

    @Ignore
    @Test
    public void testTooManyFailures() throws Exception {
        final AsyncFuture<Object> result = testSequence(failed, failed, failed);

        Assert.assertTrue(result.isDone());

        try {
            result.getNow();
        } catch (ExecutionException e) {
            Assert.assertNotNull(e.getCause());
            Assert.assertArrayEquals(new Throwable[] { CAUSE, CAUSE, CAUSE }, e.getCause().getSuppressed());
            return;
        }

        Assert.fail("exception not thrown");
    }

    @Test
    public void testClosedByFuture() throws Exception {
        Assert.assertEquals(1, c.refCount());
        Mockito.verify(delegate, Mockito.never()).close();
        Mockito.verify(delegate, Mockito.never()).release();
        Assert.assertFalse(c.isCloseRequested());

        closed.finished();

        Assert.assertEquals(0, c.refCount());
        Mockito.verify(delegate).close();
        Mockito.verify(delegate).release();
        Assert.assertTrue(c.isCloseRequested());
    }

    @Test
    public void testClosed() throws Exception {
        Assert.assertEquals(1, c.refCount());
        Mockito.verify(delegate, Mockito.never()).close();
        Mockito.verify(delegate, Mockito.never()).release();
        Assert.assertFalse(c.isCloseRequested());

        c.close();

        Assert.assertEquals(0, c.refCount());
        Mockito.verify(delegate).close();
        Mockito.verify(delegate).release();
        Assert.assertTrue(c.isCloseRequested());
    }
}
