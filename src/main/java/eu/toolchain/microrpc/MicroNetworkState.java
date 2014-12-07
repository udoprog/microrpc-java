package eu.toolchain.microrpc;

import eu.toolchain.microrpc.timer.MicroThreadTimer;
import eu.toolchain.microrpc.timer.Task;
import eu.toolchain.microrpc.timer.TaskSchedule;

public interface MicroNetworkState {
    public void shutdown() throws InterruptedException;

    public MicroThreadTimer timer();

    public TaskSchedule scheduleTimeout(Task task);

    public TaskSchedule scheduleKeepAlive(Task task);
}
