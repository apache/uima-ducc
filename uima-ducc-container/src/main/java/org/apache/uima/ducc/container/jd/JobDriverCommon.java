package org.apache.uima.ducc.container.jd;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.uima.ducc.container.common.DuccLogger;
import org.apache.uima.ducc.container.common.IDuccId;
import org.apache.uima.ducc.container.common.IDuccLogger;
import org.apache.uima.ducc.container.jd.dispatch.IRemoteWorkerIdentity;
import org.apache.uima.ducc.container.jd.dispatch.IWorkItem;

public class JobDriverCommon {

	private static IDuccLogger logger = DuccLogger.getLogger(JobDriverCommon.class, IDuccLogger.Component.JD.name());
	
	private static JobDriverCommon instance = null;
	
	public static JobDriverCommon getInstance() {
		return instance;
	}
	
	private ConcurrentHashMap<IRemoteWorkerIdentity, IWorkItem> map = null;
	private JobDriverCasManager jdcm = null;
	
	public JobDriverCommon(String[] classpath, String crXml, String crCfg) {
		initialize(classpath, crXml, crCfg);
		if(instance == null) {
			instance = this;
		}
	}
	
	public void initialize(String[] classpath, String crXml, String crCfg) {
		String location = "initialize";
		try {
			map = new ConcurrentHashMap<IRemoteWorkerIdentity, IWorkItem>();
			jdcm = new JobDriverCasManager(classpath, crXml, crCfg);
		}
		catch(Exception e) {
			logger.error(location, IDuccId.null_id, e);
		}
		
	}
	
	public ConcurrentHashMap<IRemoteWorkerIdentity, IWorkItem> getMap() {
		return map;
	}
	
	public JobDriverCasManager getCasManager() {
		return jdcm;
	}
}
