package org.apache.uima.ducc.container.jd.dispatch;

import org.apache.uima.ducc.container.common.DuccLogger;
import org.apache.uima.ducc.container.common.IDuccId;
import org.apache.uima.ducc.container.common.IDuccLogger;
import org.apache.uima.ducc.container.net.iface.IMetaCasRequester;

public class RemoteWorkerIdentity implements IRemoteWorkerIdentity, Comparable<Object> {
	
	private IDuccLogger logger = DuccLogger.getLogger(RemoteWorkerIdentity.class, IDuccLogger.Component.JD.name());
	
	private String node = null;
	private int pid = 0;
	private int tid = 0;
	
	public RemoteWorkerIdentity(IMetaCasRequester metaCasRequester) {
		setNode(metaCasRequester.getRequesterName());
		setPid(metaCasRequester.getRequesterProcessId());
		setTid(metaCasRequester.getRequesterThreadId());
	}
	
	public RemoteWorkerIdentity(String node, int pid, int tid) {
		setNode(node);
		setPid(pid);
		setTid(tid);
	}
	
	@Override
	public String toString() {
		StringBuffer sb = new StringBuffer();
		if(node != null) {
			sb.append(node);
			sb.append(".");
		}
		sb.append(pid);
		sb.append(".");
		sb.append(tid);
		return sb.toString();
	}
	
	@Override
	public String getNode() {
		return node;
	}
	
	@Override
	public void setNode(String value) {
		node = value;
	}
	
	@Override
	public int getPid() {
		return pid;
	}
	
	@Override
	public void setPid(int value) {
		pid = value;
	}
	
	@Override
	public int getTid() {
		return tid;
	}
	
	@Override
	public void setTid(int value) {
		tid = value;
	}
	
	private int compareNode(RemoteWorkerIdentity that) {
		int retVal = 0;
		String thisNode = this.getNode();
		String thatNode = that.getNode();
		if(thisNode != null) {
			if(thatNode != null) {
				retVal = thisNode.compareTo(thatNode);
			}
		}
		return retVal;
	}
	
	private int comparePid(RemoteWorkerIdentity that) {
		int retVal = 0;
		Integer thisPid = new Integer(this.pid);
		Integer thatPid = new Integer(that.pid);
		retVal = thisPid.compareTo(thatPid);
		return retVal;
	}
	
	private int compareTid(RemoteWorkerIdentity that) {
		int retVal = 0;
		Integer thisTid = new Integer(this.tid);
		Integer thatTid = new Integer(that.tid);
		retVal = thisTid.compareTo(thatTid);
		return retVal;
	}
	
	@Override
	public int compareTo(Object o) {
		String location = "compareTo";
		int retVal = 0;
		try {
			if(o != null) {
				RemoteWorkerIdentity that = (RemoteWorkerIdentity) o;
				if(retVal == 0) {
					retVal = compareNode(that);
				}
				if(retVal == 0) {
					retVal = comparePid(that);
				}
				if(retVal == 0) {
					retVal = compareTid(that);
				}
			}
		}
		catch(Exception e) {
			logger.error(location, IDuccId.null_id, e);
		}
		return retVal;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		String thisNode = this.getNode();
		Integer thisPid = new Integer(this.pid);
		Integer thisTid = new Integer(this.tid);
		result = prime * result + ((thisNode == null) ? 0 : thisNode.hashCode());
		result = prime * result + ((thisPid == null) ? 0 : thisPid.hashCode());
		result = prime * result + ((thisTid == null) ? 0 : thisTid.hashCode());
		return result;
	}
	
	@Override
	public boolean equals(Object obj) {
		String location = "equals";
		boolean retVal = false;
		try {
			if(obj != null) {
				if(this == obj) {
					retVal = true;
				}
				else {
					RemoteWorkerIdentity that = (RemoteWorkerIdentity) obj;
					if(this.compareTo(that) == 0) {
						retVal = true;
					}
				}
			}
		}
		catch(Exception e) {
			logger.error(location, IDuccId.null_id, e);
		}
		return retVal;
	}
}
