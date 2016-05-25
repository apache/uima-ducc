package org.apache.uima.ducc.common.persistence.rm;

import org.apache.uima.ducc.common.Node;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.utils.id.DuccId;

public interface IDbShare {


	public abstract int getNodepoolDepth();

	public abstract String getNodepoolId();

	public abstract DuccId getId();

	// UIMA-4142
	public abstract boolean isBlacklisted();

	// UIMA-4142
	public abstract DuccId getBlJobId();

	public abstract NodeIdentity getNodeIdentity();

	public abstract Node getNode();

	/**
	 * The order of the share itself.
	 */
	public abstract int getShareOrder();

	/**
	 * Returns only initialization time.  Eventually getInvestment() may take other things into
	 * consideration so we separate these two (even though currently they do the same thing.)
	 */
	public abstract long getInitializationTime();

	public abstract void setInitializationTime(long millis);

	public abstract void setFixed();

	public abstract boolean isPurged();
    public abstract boolean isEvicted();
    public abstract boolean isFixed();

}
