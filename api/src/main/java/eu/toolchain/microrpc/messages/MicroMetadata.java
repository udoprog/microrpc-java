package eu.toolchain.microrpc.messages;

import java.util.UUID;

import lombok.Data;

@Data
public class MicroMetadata implements MicroMessage {
    private final UUID remoteId;
}