package eu.toolchain.microrpc.messages;

import java.util.UUID;

import lombok.Data;

/**
 * Message sent to indicate that a request has timed out.
 *
 * This is sent from client-to-server to at least give the server a chance to clean up resources.
 *
 * @author udoprog
 */
@Data
public class MicroRequestTimeout implements MicroMessage {
    private final UUID requestId;
}