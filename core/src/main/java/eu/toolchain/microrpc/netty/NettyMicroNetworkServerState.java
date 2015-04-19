package eu.toolchain.microrpc.netty;

import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import lombok.RequiredArgsConstructor;
import eu.toolchain.microrpc.MicroNetworkState;
import eu.toolchain.microrpc.timer.MicroThreadTimer;
import eu.toolchain.microrpc.timer.MicroTimer;

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
    public MicroTimer.TaskSchedule scheduleTimeout(MicroTimer.Task task) {
        return timer.schedule("micro-idle-timeout", idleTimeout, task);
    }

    @Override
    public MicroTimer.TaskSchedule scheduleRequestHeartbeat(final MicroTimer.Task task) {
        return timer.schedule("micro-heartbeat", keepAliveTime, new MicroTimer.Task() {
            @Override
            public void run(MicroTimer.TaskSchedule t) throws Exception {
                t.delay(keepAliveTime);
                task.run(t);
            }
        });
    }
}