package eu.toolchain.microrpc;

import java.util.UUID;

import eu.toolchain.async.AsyncFramework;
import eu.toolchain.microrpc.messages.MicroMessage;
import eu.toolchain.serializer.Serializer;

public interface MicroNetworkConfig {
    MicroNetwork setup(AsyncFramework async, Serializer<MicroMessage> message, MicroReceiver receiver,
            MicroConnectionProvider connectionProvider, UUID localId);
}