package eu.toolchain.microrpc;

import eu.toolchain.microrpc.timer.MicroTimer;

public interface MicroNetworkState {
    public void shutdown() throws InterruptedException;

    public MicroTimer timer();

    public MicroTimer.TaskSchedule scheduleTimeout(MicroTimer.Task task);

    public MicroTimer.TaskSchedule scheduleRequestHeartbeat(MicroTimer.Task task);
}
