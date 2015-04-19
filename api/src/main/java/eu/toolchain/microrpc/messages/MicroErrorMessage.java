package eu.toolchain.microrpc.messages;

import java.util.UUID;

import lombok.Data;

@Data
public class MicroErrorMessage implements MicroResponseMessage {
    private final UUID requestId;
    private final String message;

    @Override
    public UUID requestId() {
        return requestId;
    }
}
