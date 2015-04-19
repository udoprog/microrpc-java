package eu.toolchain.microrpc.timer;

public interface MicroTimer {
    /**
     * Schedule a named task in the future.
     *
     * @param name Name of the task.
     * @param delay Time in the future for the task to execute.
     * @param task Task to execute.
     * @return A scheduled task reference.
     */
    public TaskSchedule schedule(String name, long delay, Task task);

    /**
     * Schedule a task to execute in the future.
     *
     * Same as {@link #schedule(String, long, Task)} but using a default name.
     *
     * @see #schedule(String, long, Task)
     */
    public TaskSchedule schedule(long delay, Task task);

    /**
     * A task being scheduled.
     */
    public interface Task {
        public void run(TaskSchedule t) throws Exception;
    }

    /**
     * A task that has been timed.
     */
    public interface TaskSchedule {
        /**
         * Cancel the current task.
         *
         * This will prevent the task from being executed.
         */
        public void cancel();

        /**
         * Delay the current task.
         *
         * @param delay The amount of time that the task should be delayed. Zero values will be treated as a no-op.
         * @throws IllegalArgumentException If delay is negative.
         */
        public void delay(long delay);

        /**
         * Delay the task until the exact time in milliseconds from the unix epoch.
         * 
         * @param when The time until which this task is delayed.
         */
        public void delayUntil(long when);

        /**
         * Fetch the underlying task that is being scheduled.
         *
         * @return The task being scheduled.
         */
        public Task task();
    }
}