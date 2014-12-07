package eu.toolchain.microrpc.serializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import eu.toolchain.microrpc.messages.MicroErrorMessage;
import eu.toolchain.microrpc.messages.MicroMessage;
import eu.toolchain.microrpc.messages.MicroMetadata;
import eu.toolchain.microrpc.messages.MicroRequest;
import eu.toolchain.microrpc.messages.MicroRequestKeepAlive;
import eu.toolchain.microrpc.messages.MicroRequestResponse;
import eu.toolchain.microrpc.messages.MicroRequestTimeout;
import eu.toolchain.serializer.SerialReader;
import eu.toolchain.serializer.SerialWriter;
import eu.toolchain.serializer.Serializer;
import eu.toolchain.serializer.SerializerFramework;
import eu.toolchain.serializer.SerializerFramework.TypeMapping;

public class MicroSerializers {
    private final Serializer<MicroRequest> request;
    private final Serializer<MicroRequestKeepAlive> requestKeepAlive;
    private final Serializer<MicroRequestTimeout> requestTimeout;
    private final Serializer<MicroRequestResponse> response;
    private final Serializer<MicroErrorMessage> errorMessage;
    private final Serializer<MicroMetadata> metadata;
    private final Serializer<MicroMessage> message;

    public MicroSerializers(final SerializerFramework s) {
        this.request = new Serializer<MicroRequest>() {
            private final Serializer<Integer> version = s.nullable(s.integer());
            private final Serializer<UUID> uuid = s.uuid();
            private final Serializer<String> string = s.string();
            private final Serializer<byte[]> byteArray = s.byteArray();

            @Override
            public void serialize(SerialWriter buffer, MicroRequest value) throws IOException {
                version.serialize(buffer, value.getVersion());
                uuid.serialize(buffer, value.getId());
                string.serialize(buffer, value.getEndpoint());
                byteArray.serialize(buffer, value.getBody());
            }

            @Override
            public MicroRequest deserialize(SerialReader buffer) throws IOException {
                final Integer version = this.version.deserialize(buffer);
                final UUID id = uuid.deserialize(buffer);
                final String endpoint = string.deserialize(buffer);
                final byte[] body = byteArray.deserialize(buffer);
                return new MicroRequest(version, id, endpoint, body);
            }
        };

        this.requestKeepAlive = new Serializer<MicroRequestKeepAlive>() {
            private final Serializer<UUID> uuid = s.uuid();

            @Override
            public void serialize(SerialWriter buffer, MicroRequestKeepAlive value) throws IOException {
                uuid.serialize(buffer, value.getRequestId());
            }

            @Override
            public MicroRequestKeepAlive deserialize(SerialReader buffer) throws IOException {
                final UUID requestId = uuid.deserialize(buffer);
                return new MicroRequestKeepAlive(requestId);
            }
        };

        this.requestTimeout = new Serializer<MicroRequestTimeout>() {
            private final Serializer<UUID> uuid = s.uuid();

            @Override
            public void serialize(SerialWriter buffer, MicroRequestTimeout value) throws IOException {
                uuid.serialize(buffer, value.getRequestId());
            }

            @Override
            public MicroRequestTimeout deserialize(SerialReader buffer) throws IOException {
                final UUID requestId = uuid.deserialize(buffer);
                return new MicroRequestTimeout(requestId);
            }
        };

        this.response = new Serializer<MicroRequestResponse>() {
            private final Serializer<Integer> version = s.nullable(s.integer());
            private final Serializer<UUID> uuid = s.uuid();
            private final Serializer<byte[]> byteArray = s.byteArray();

            @Override
            public void serialize(SerialWriter buffer, MicroRequestResponse value) throws IOException {
                version.serialize(buffer, value.getVersion());
                uuid.serialize(buffer, value.getRequestId());
                byteArray.serialize(buffer, value.getBody());
            }

            @Override
            public MicroRequestResponse deserialize(SerialReader buffer) throws IOException {
                final Integer version = this.version.deserialize(buffer);
                final UUID requestId = uuid.deserialize(buffer);
                final byte[] body = byteArray.deserialize(buffer);
                return new MicroRequestResponse(version, requestId, body);
            }
        };

        this.errorMessage = new Serializer<MicroErrorMessage>() {
            private final Serializer<UUID> uuid = s.uuid();
            private final Serializer<String> string = s.nullable(s.string());

            @Override
            public void serialize(SerialWriter buffer, MicroErrorMessage value) throws IOException {
                uuid.serialize(buffer, value.getRequestId());
                string.serialize(buffer, value.getMessage());
            }

            @Override
            public MicroErrorMessage deserialize(SerialReader buffer) throws IOException {
                final UUID requestId = this.uuid.deserialize(buffer);
                final String message = this.string.deserialize(buffer);
                return new MicroErrorMessage(requestId, message);
            }
        };

        this.metadata = new Serializer<MicroMetadata>() {
            private final Serializer<UUID> uuid = s.uuid();

            @Override
            public void serialize(SerialWriter buffer, MicroMetadata value) throws IOException {
                uuid.serialize(buffer, value.getRemoteId());
            }

            @Override
            public MicroMetadata deserialize(SerialReader buffer) throws IOException {
                final UUID remoteId = uuid.deserialize(buffer);
                return new MicroMetadata(remoteId);
            }
        };

        final List<TypeMapping<? extends MicroMessage>> types = new ArrayList<>();

        types.add(s.type(0x01, MicroRequest.class, request()));
        types.add(s.type(0x02, MicroRequestKeepAlive.class, requestKeepAlive()));
        types.add(s.type(0x03, MicroRequestTimeout.class, requestTimeout()));
        types.add(s.type(0x04, MicroRequestResponse.class, response()));
        types.add(s.type(0x05, MicroErrorMessage.class, errorMessage()));
        types.add(s.type(0x06, MicroMetadata.class, metadata()));

        this.message = s.subtypes(types);
    }

    public Serializer<MicroRequest> request() {
        return request;
    }

    public Serializer<MicroRequestKeepAlive> requestKeepAlive() {
        return requestKeepAlive;
    }

    public Serializer<MicroRequestTimeout> requestTimeout() {
        return requestTimeout;
    }

    public Serializer<MicroRequestResponse> response() {
        return response;
    }

    public Serializer<MicroErrorMessage> errorMessage() {
        return errorMessage;
    }

    public Serializer<MicroMetadata> metadata() {
        return metadata;
    }

    public Serializer<MicroMessage> message() {
        return message;
    }
}
