package eu.toolchain.microrpc.messages;

import java.util.UUID;

import lombok.Data;
import lombok.ToString;

@Data
@ToString(exclude = { "body" })
public class MicroRequest implements MicroMessage {
    private final Integer version;
    private final UUID id;
    private final String endpoint;
    private final byte[] body;

    public UUID id() {
        return id;
    }
}