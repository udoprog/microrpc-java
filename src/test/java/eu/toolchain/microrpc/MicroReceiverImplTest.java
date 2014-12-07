package eu.toolchain.microrpc;

import java.util.UUID;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import eu.toolchain.microrpc.messages.MicroErrorMessage;
import eu.toolchain.microrpc.messages.MicroMessage;
import eu.toolchain.microrpc.messages.MicroRequestResponse;

public class MicroReceiverImplTest {
    private final UUID id = UUID.randomUUID();
    private final String message = "message";
    private final int version = 0xff;
    private final byte[] body = new byte[0];

    private final MicroErrorMessage error = new MicroErrorMessage(id, message);
    private final MicroRequestResponse requestResult = new MicroRequestResponse(version, id, body);

    private MicroReceiverProvider provider;
    private MicroRouter router;
    private MicroReceiverImpl impl;
    private MicroConnection c;

    @Before
    public void setup() {
        provider = Mockito.mock(MicroReceiverProvider.class);
        router = Mockito.mock(MicroRouter.class);
        impl = new MicroReceiverImpl(provider, router);
        c = Mockito.mock(MicroConnection.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUnhandledMessage() throws Exception {
        impl.receive(c, Mockito.mock(MicroMessage.class));
    }

    @Test
    public void testErrorMessage() throws Exception {
        final MicroPendingRequest pending = Mockito.mock(MicroPendingRequest.class);

        Mockito.when(provider.pending(id)).thenReturn(pending);
        impl.receive(c, error);
        Mockito.verify(pending).resolve(error);
    }

    @Test
    public void testErrorMessageNoPending() throws Exception {
        final MicroPendingRequest pending = Mockito.mock(MicroPendingRequest.class);

        Mockito.when(provider.pending(id)).thenReturn(null);
        impl.receive(c, error);
        Mockito.verify(pending, Mockito.never()).resolve(error);
    }

    @Test
    public void testRequestResult() throws Exception {
        final MicroPendingRequest pending = Mockito.mock(MicroPendingRequest.class);

        Mockito.when(provider.pending(id)).thenReturn(pending);
        impl.receive(c, requestResult);
        Mockito.verify(pending).resolve(requestResult);
    }

    @Test
    public void testRequestResultNoPending() throws Exception {
        final MicroPendingRequest pending = Mockito.mock(MicroPendingRequest.class);

        Mockito.when(provider.pending(id)).thenReturn(null);
        impl.receive(c, requestResult);
        Mockito.verify(pending, Mockito.never()).resolve(requestResult);
    }
}
