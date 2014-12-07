package eu.toolchain.microrpc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import com.google.common.io.ByteStreams;

import eu.toolchain.async.AsyncFuture;
import eu.toolchain.async.Transform;
import eu.toolchain.microrpc.messages.MicroErrorMessage;
import eu.toolchain.microrpc.messages.MicroMessage;
import eu.toolchain.microrpc.messages.MicroRequest;
import eu.toolchain.microrpc.messages.MicroRequestKeepAlive;
import eu.toolchain.microrpc.messages.MicroRequestResponse;
import eu.toolchain.microrpc.messages.MicroRequestTimeout;
import eu.toolchain.microrpc.messages.MicroResponseMessage;
import eu.toolchain.microrpc.serializer.DataOutputSerialWriter;

@Slf4j
@RequiredArgsConstructor
public class MicroReceiverImpl implements MicroReceiver {
    private final Map<Class<? extends MicroMessage>, MessageHandle<? extends MicroMessage>> handles = new HashMap<>();

    private final MicroReceiverProvider provider;
    private final MicroRouter router;

    private static interface MessageHandle<T> {
        void handle(MicroConnection c, T message) throws Exception;
    }

    private <T extends MicroMessage> void handle(Class<? extends T> type, MessageHandle<T> handle) {
        handles.put(type, handle);
    }

    {
        handle(MicroRequest.class, new MessageHandle<MicroRequest>() {
            @Override
            public void handle(final MicroConnection c, MicroRequest request) throws IOException {
                final MicroVersionMapping m = router.resolve(request.getVersion());

                if (m == null) {
                    c.send(new MicroErrorMessage(request.getId(), request.toString()
                            + ": no endpoint registered for version"));
                    return;
                }

                try {
                    provider.request(c, m, request.getId(), callHandle(m, request));
                } catch (Exception e) {
                    c.send(new MicroErrorMessage(request.getId(), e.getMessage()));
                }
            }
        });

        handle(MicroRequestKeepAlive.class, new MessageHandle<MicroRequestKeepAlive>() {
            @Override
            public void handle(MicroConnection c, MicroRequestKeepAlive keepAlive) throws Exception {
                provider.keepAlive(keepAlive.getRequestId());
            }
        });

        handle(MicroRequestTimeout.class, new MessageHandle<MicroRequestTimeout>() {
            @Override
            public void handle(MicroConnection c, MicroRequestTimeout timeout) throws Exception {
                final MicroProcessingRequest processing = provider.removeProcessing(timeout.getRequestId());

                if (processing != null)
                    processing.fail(new Exception("request timed out"));
            }
        });

        final MessageHandle<MicroResponseMessage> response = new MessageHandle<MicroResponseMessage>() {
            @Override
            public void handle(MicroConnection c, MicroResponseMessage response) throws Exception {
                final MicroPendingRequest p = provider.pending(response.requestId());

                if (p != null)
                    p.resolve(response);
            }
        };

        handle(MicroRequestResponse.class, response);
        handle(MicroErrorMessage.class, response);
    }

    /**
     * Handle for receiving all other state-less messages.
     */
    @Override
    public void receive(final MicroConnection c, final MicroMessage message) throws Exception {
        if (log.isDebugEnabled())
            log.debug("<= {} {}", c, message);

        @SuppressWarnings("unchecked")
        final MessageHandle<MicroMessage> handle = (MessageHandle<MicroMessage>) handles.get(message.getClass());

        if (handle == null)
            throw new IllegalArgumentException("no handler for message: " + message);

        handle.handle(c, message);
    }

    private AsyncFuture<MicroMessage> callHandle(final MicroVersionMapping m, final MicroRequest request)
            throws Exception {
        final MicroEndpointMapping<?, ?> handle = m.endpoint(request.getEndpoint());

        if (handle == null)
            throw new IllegalArgumentException(request + ": no such endpoint");

        final Object query = handle.deserialize(request.getBody());

        final AsyncFuture<Object> response = handle.query(query);

        if (response == null)
            throw new IllegalArgumentException(request + ": handle returned null response");

        return response.transform(transformResponse(handle, request.getId())).error(errorResponse(m, request));
    }

    private Transform<Throwable, MicroMessage> errorResponse(final MicroVersionMapping m, final MicroRequest request) {
        return new Transform<Throwable, MicroMessage>() {
            @Override
            public MicroMessage transform(Throwable e) throws Exception {
                log.error(request + ": request failed", e);
                return new MicroErrorMessage(request.getId(), e.getMessage());
            }
        };
    }

    private Transform<Object, MicroMessage> transformResponse(final MicroEndpointMapping<?, ?> handle, final UUID id) {
        return new Transform<Object, MicroMessage>() {
            @Override
            public MicroMessage transform(Object result) throws Exception {
                final DataOutputSerialWriter out = new DataOutputSerialWriter(ByteStreams.newDataOutput());
                handle.serialize(out, result);
                return handle.result(id, out.toByteArray());
            }
        };
    }
}
