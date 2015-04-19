package eu.toolchain.microrpc.timer;

import java.util.PriorityQueue;
import java.util.concurrent.ScheduledExecutorService;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

// @formatter:off
/**
 * Timer implementation that runs in a single thread and defers tasks to be executed to a thread pool.
 *
 * The primary purpose for this task is the ability to 'delay' execution very cheaply for a specific task.
 *
 * Also, causing a delay inside of an executing task is a simple measure for creating an interval timer.
 *
 * <h1>Example usage</h1>
 *
 * <pre>
 * {@code
 * final ScheduledExecutorService executor = Executors.newScheduledThreadPool(10);
 *
 * final ThreadTimer thread = new ThreadTimer(executor);
 * thread.start();
 * 
 * thread.schedule(1000, new Task() {
 *     public void run(TimerTask t) throws Exception {
 *         System.out.println(Thread.currentThread() + ": A");
 *     }
 * }).delay(2000);
 * 
 * final TimerTask b = thread.schedule(4000, new Task() {
 *     public void run(TimerTask t) throws Exception {
 *         System.out.println(Thread.currentThread() + ": B (again)");
 *         t.delay(1000);
 *     }
 * });
 * 
 * thread.schedule(6500, new Task() {
 *     public void run(TimerTask t) throws Exception {
 *         System.out.println(Thread.currentThread() + ": C cancel B");
 *         b.cancel();
 *     }
 * });
 * 
 * thread.schedule(10000, new Task() {
 *     public void run(TimerTask t) throws Exception {
 *         System.out.println(Thread.currentThread() + ": Goodbye");
 *         thread.shutdown();
 *     }
 * });
 * 
 * thread.join();
 * executor.shutdown();
 * }
 * </pre>
 */
// @formatter:on
@Slf4j
public class MicroThreadTimer extends Thread implements MicroTimer {
    /**
     * The default time in milliseconds that a task is allowed to 'be off' in order to be executed.
     */
    public static final long DEFAULT_TOLERANCE = 10;

    private final ScheduledExecutorService executor;
    private final long tolerance;

    public MicroThreadTimer(ScheduledExecutorService executor) {
        this(executor, DEFAULT_TOLERANCE);
    }

    /**
     * @param executor The executor service to use when executing tasks.
     * @param tolerance The allowed tolerance in milliseconds that a task may be off in order to be executed.
     */
    public MicroThreadTimer(ScheduledExecutorService executor, long tolerance) {
        this.executor = executor;
        this.tolerance = tolerance;
    }

    /**
     * Global lock.
     */
    private final Object lock = new Object();
    private final PriorityQueue<TimeTaskEntry> timeouts = new PriorityQueue<TimeTaskEntry>();
    private volatile boolean notified = false;
    private volatile boolean stopped = false;

    private TimeTaskEntry schedule(long when, TimedTaskImpl task) {
        final TimeTaskEntry e = new TimeTaskEntry(when, task);

        synchronized (lock) {
            this.timeouts.add(e);
            this.notified = true;
            this.lock.notify();
        }

        return e;
    }

    @Override
    public TaskSchedule schedule(String name, long delay, Task task) {
        final long when = now() + delay;
        final TimedTaskImpl t = new TimedTaskImpl(name, this, task);
        t.entry = schedule(when, t);
        return t;
    }

    @Override
    public TaskSchedule schedule(long delay, Task task) {
        final long when = now() + delay;
        final TimedTaskImpl t = new TimedTaskImpl(null, this, task);
        t.entry = schedule(when, t);
        return t;
    }

    public void shutdown() {
        synchronized (lock) {
            this.stopped = true;
            this.notified = true;
            this.lock.notify();
        }
    }

    @Override
    public void run() {
        synchronized (lock) {
            while (!stopped) {
                try {
                    tick();
                } catch (InterruptedException e) {
                    log.error("tick interrupted", e);
                }
            }
        }
    }

    private void tick() throws InterruptedException {
        final TimeTaskEntry entry = timeouts.poll();

        if (entry == null) {
            lock.wait();
            return;
        }

        long delay;

        while ((delay = entry.when - now()) > tolerance) {
            notified = false;
            lock.wait(delay);

            if (stopped)
                return;

            // thread was notified that new tasks are available.
            if (notified) {
                timeouts.add(entry);
                return;
            }
        }

        entry.task.schedule(entry, executor);
    }

    /**
     * Get current time.
     * 
     * @return The current time in milliseconds.
     */
    private long now() {
        return System.currentTimeMillis();
    }

    @RequiredArgsConstructor
    public static final class TimedTaskImpl implements TaskSchedule, Runnable {
        /**
         * Lock for this timed task.
         * 
         * Must be acquired when reading or modifying {@link #entry}.
         */
        private final Object lock = new Object();

        /**
         * Name of the given task.
         */
        private final String name;

        private final MicroThreadTimer thread;
        private final Task task;

        private volatile TimeTaskEntry entry;

        @Override
        public void cancel() {
            synchronized (lock) {
                this.entry = null;
            }
        }

        /**
         * Attempts to schedule this task given the provided entry.
         *
         * @param entry The entry that is due to be executed.
         * @param executor The executor on which the task will be executed.
         */
        public void schedule(TimeTaskEntry entry, ScheduledExecutorService executor) {
            synchronized (lock) {
                // given entry was cancelled.
                if (entry != this.entry)
                    return;
            }

            executor.submit(this);
        }

        @Override
        public void delay(long delay) {
            if (delay <= 0)
                throw new IllegalArgumentException("delay cannot be negative");

            synchronized (lock) {
                // cannot be delayed, already cancelled.
                if (entry == null)
                    return;

                delayUntil(entry.when + delay);
            }
        }

        @Override
        public void delayUntil(long when) {
            synchronized (lock) {
                // schedule a new task, with this content.
                this.entry = thread.schedule(when, this);
            }
        }

        @Override
        public Task task() {
            return task;
        }

        public void run() {
            try {
                task.run(this);
            } catch (Exception e) {
                log.error("Exception in task {}", name(), e);
            }
        }

        public String name() {
            return name == null ? "unknown" : name;
        }
    }

    @AllArgsConstructor
    private static final class TimeTaskEntry implements Comparable<TimeTaskEntry> {
        /**
         * When to execute entry.
         */
        private final long when;

        /**
         * The task for this entry.
         *
         * Each task carries a volatile reference back to their corresponding entry.
         *
         * If this does not match, it indicates that a specific task has either been rescheduled or cancelled, in which
         * case it should not be executed.
         */
        private final TimedTaskImpl task;

        @Override
        public int compareTo(TimeTaskEntry o) {
            return Long.compare(when, o.when);
        }
    }
}