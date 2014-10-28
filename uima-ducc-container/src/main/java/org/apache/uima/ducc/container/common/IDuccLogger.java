package org.apache.uima.ducc.container.common;


public interface IDuccLogger {

	public enum Component { JD, OR, WS };

	public void debug(String location, IDuccId jobid, Object ... args);
	public void error(String location, IDuccId jobid, Object ... args);
	public void info(String location, IDuccId jobid, Object ... args);
}
