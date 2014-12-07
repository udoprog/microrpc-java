package eu.toolchain.microrpc.timer;

/**
 * A task being scheduled.
 */
public interface Task {
    public void run(TaskSchedule t) throws Exception;
}