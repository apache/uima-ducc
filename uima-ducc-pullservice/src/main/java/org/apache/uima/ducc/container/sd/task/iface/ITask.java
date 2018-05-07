package org.apache.uima.ducc.container.sd.task.iface;

public interface ITask {
	// returns true if no work found, false otherwise
	public boolean isEmpty();
	// returns stringified task
	public String asString();
	// returns application specific data which may be used to 
	// correlate service responses. 
	public String getMetadata();
}
