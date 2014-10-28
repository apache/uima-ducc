package org.apache.uima.ducc.container.net.iface;

import java.io.Serializable;

import org.apache.uima.ducc.container.net.impl.TransactionId;

public interface IMetaCasTransaction extends IMetaCasProvider, IMetaCasRequester, Serializable {

	public enum Type { Get, Ack, End };
	
	public Type getType();
	public void setType(Type value);
	
	public enum Direction { Request, Response };
	
	public Direction getDirection();
	public void setDirection(Direction value);
	
	public TransactionId getTransactionId();
	public void setTransactionId(TransactionId value);
	
	public IMetaCas getMetaCas();
	public void setMetaCas(IMetaCas value);
}
