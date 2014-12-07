package eu.toolchain.microrpc.connection;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;
import eu.toolchain.microrpc.MicroConnection;

@Slf4j
public abstract class AbstractMicroConnection implements MicroConnection {
    /**
     * The reference count for the current object.
     *
     * When Reaching zero, will cause the reference object to be deallocated with the 'dealloc' method.
     *
     * @see #dealloc()
     */
    private final AtomicInteger references = new AtomicInteger(1);

    /**
     * Asserts that close can only be requested once, which will decrease the above reference count by one.
     */
    private final AtomicBoolean closeRequested = new AtomicBoolean(false);

    @Override
    public void close() {
        log.debug("close() {}", this);

        if (closeRequested.compareAndSet(false, true))
            release();
    }

    @Override
    public final boolean isCloseRequested() {
        return closeRequested.get();
    }

    @Override
    public final void retain() {
        final int r = references.incrementAndGet();

        if (log.isDebugEnabled())
            log.debug("retain() {} = {}", this, r);
    }

    @Override
    public final int refCount() {
        return references.get();
    }

    @Override
    public final boolean release() {
        final int r = references.addAndGet(-1);

        if (log.isDebugEnabled())
            log.debug("release() {} = {}", this, r);

        if (r < 0)
            throw new IllegalStateException("bug: reference count must not be negative");

        if (r == 0) {
            if (log.isDebugEnabled())
                log.debug("dealloc() {}", this);

            dealloc();
            return true;
        }

        return false;
    }

    /**
     * Implement to handle 'de-allocation' of the current connection.
     *
     * de-allocation happens when the reference count for this connection reaches zero, and is guaranteed to only fire
     * once.
     */
    abstract protected void dealloc();
}
