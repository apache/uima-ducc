package org.apache.uima.ducc.container.common;


public interface IContainerLogger {

	public enum Component { JD, JP };

	public void debug(String location, IEntityId jobid, Object ... args);
	public void error(String location, IEntityId jobid, Object ... args);
	public void info(String location, IEntityId jobid, Object ... args);
	public void trace(String location, IEntityId jobid, Object ... args);
}
