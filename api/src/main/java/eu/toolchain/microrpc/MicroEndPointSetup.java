package eu.toolchain.microrpc;

import eu.toolchain.serializer.Serializer;

public interface MicroEndPointSetup {
    /**
     * Register end point that takes a body and returns a response.
     *
     * This form will register the end point for the default version.
     *
     * @param endpoint Name of end point.
     * @param query Query serializer.
     * @param response Response serializer.
     * @param handle Handle method.
     */
    public <Q, R> void on(String endpoint, Serializer<Q> query, Serializer<R> response, MicroEndPoint<Q, R> handle);

    /**
     * Register end point that does not take a body and returns a response.
     *
     * @param endpoint Name of endpoint.
     * @param response Response serializer.
     * @param handle Handle method.
     */
    public <R> void on(String endpoint, Serializer<R> response, MicroEndPoint<Void, R> handle);
}
