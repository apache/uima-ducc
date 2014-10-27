package org.apache.uima.ducc.user.jd.iface;

import java.util.Properties;

public class JdUserErrorHandlerDirective implements IJdUserErrorHandlerDirective {

	private boolean terminateJob = false;
	private boolean terminateProcess = false;
	private boolean retryCas = false;
	
	private String reasonTerminateJob = null;
	private String reasonTerminateProcess = null;
	private String reasonRetryCas = null;
	
	
	@Override
	public boolean isTerminateJob() {
		return terminateJob;
	}

	@Override
	public boolean isTerminateProcess() {
		return terminateProcess;
	}

	@Override
	public boolean isRetryCas() {
		return retryCas;
	}

	@Override
	public String getReasonTerminateJob() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getReasonTerminateProcess() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getReasonRetryCas() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void config(Properties properties) {
		// TODO Auto-generated method stub
		
	}

}
