package eu.toolchain.microrpc.messages;

import java.util.UUID;

import lombok.Data;

/**
 * Message sent to indicate that a request is still being processed.
 *
 * Will be sent periodically to prevent requests from being marked as failed.
 *
 * @author udoprog
 */
@Data
public class MicroRequestKeepAlive implements MicroMessage {
    private final UUID requestId;
}