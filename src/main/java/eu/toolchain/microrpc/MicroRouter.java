package eu.toolchain.microrpc;


public interface MicroRouter {
    public void listen(MicroListenerSetup setup);

    public void listen(final Integer version, MicroListenerSetup setup);

    public MicroVersionMapping resolve(Integer version);
}
