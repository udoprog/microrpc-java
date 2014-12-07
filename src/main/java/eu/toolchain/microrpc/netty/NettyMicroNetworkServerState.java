package eu.toolchain.microrpc.netty;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import lombok.RequiredArgsConstructor;
import eu.toolchain.microrpc.MicroNetworkState;
import eu.toolchain.microrpc.timer.MicroThreadTimer;
import eu.toolchain.microrpc.timer.Task;
import eu.toolchain.microrpc.timer.TaskSchedule;

@RequiredArgsConstructor
public final class NettyMicroNetworkServerState implements MicroNetworkState {
    private final Channel serverChannel;
    private final EventLoopGroup parentGroup;
    private final EventLoopGroup childGroup;
    private final MicroThreadTimer timer;

    private final long idleTimeout;
    private final long keepAliveTime;

    @Override
    public void shutdown() throws InterruptedException {
        serverChannel.close().sync();
        childGroup.shutdownGracefully();
        parentGroup.shutdownGracefully();
        timer.shutdown();
        timer.join();
    }

    public EventLoopGroup parentGroup() {
        return parentGroup;
    }

    public EventLoopGroup childGroup() {
        return childGroup;
    }

    public MicroThreadTimer timer() {
        return timer;
    }

    @Override
    public TaskSchedule scheduleTimeout(Task task) {
        return timer.schedule("micro-idle-timeout", idleTimeout, task);
    }

    @Override
    public TaskSchedule scheduleKeepAlive(final Task task) {
        return timer.schedule("micri-keep-alive", keepAliveTime, new Task() {
            @Override
            public void run(TaskSchedule t) throws Exception {
                t.delay(keepAliveTime);
                task.run(t);
            }
        });
    }
}