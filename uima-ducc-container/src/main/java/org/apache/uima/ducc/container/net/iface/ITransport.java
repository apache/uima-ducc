package org.apache.uima.ducc.container.net.iface;

public interface ITransport {

	public void sendAndReceive(IMetaCasTransaction metaCastransaction) throws TimeoutException;
}
