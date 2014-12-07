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
}
