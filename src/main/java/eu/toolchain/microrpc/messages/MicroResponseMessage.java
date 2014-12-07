package eu.toolchain.microrpc.messages;

import java.util.UUID;

/**
 * Super type for messages which are a response to other messages.
 */
public interface MicroResponseMessage extends MicroMessage {
    public UUID requestId();
}