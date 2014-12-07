package eu.toolchain.microrpc.messages;

import java.util.UUID;

import lombok.Data;
import lombok.ToString;

@Data
@ToString(exclude = { "body" })
public class MicroRequestResponse implements MicroResponseMessage {
    private final Integer version;
    private final UUID requestId;
    private final byte[] body;

    @Override
    public UUID requestId() {
        return requestId;
    }
}