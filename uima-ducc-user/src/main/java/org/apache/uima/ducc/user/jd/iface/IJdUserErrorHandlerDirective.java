package org.apache.uima.ducc.user.jd.iface;

import java.util.Properties;

public interface IJdUserErrorHandlerDirective {

	public boolean isTerminateJob();
	public boolean isTerminateProcess();
	public boolean isRetryCas();
	
	public String getReasonTerminateJob();
	public String getReasonTerminateProcess();
	public String getReasonRetryCas();
	
	public void config(Properties properties);
}
