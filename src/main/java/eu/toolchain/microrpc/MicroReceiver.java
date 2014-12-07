package eu.toolchain.microrpc;

import eu.toolchain.microrpc.messages.MicroMessage;

public interface MicroReceiver {
    public void receive(final MicroConnection c, final MicroMessage message) throws Exception;
}
