package org.apache.uima.ducc.container.jd;

import java.net.URL;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.ducc.container.common.DuccLogger;
import org.apache.uima.ducc.container.common.IDuccId;
import org.apache.uima.ducc.container.common.IDuccLogger;
import org.apache.uima.ducc.container.jd.classload.JobDriverCollectionReader;
import org.apache.uima.ducc.container.net.impl.MetaCas;

public class JobDriverCasManager {

	private IDuccLogger logger = DuccLogger.getLogger(JobDriverCasManager.class, IDuccLogger.Component.JD.name());
	
	private JobDriverCollectionReader jdcr = null;
	
	private LinkedBlockingQueue<MetaCas> cacheQueue = new LinkedBlockingQueue<MetaCas>();
	
	private AtomicInteger countGet = new AtomicInteger(0);
	private AtomicInteger countPut = new AtomicInteger(0);
	private AtomicInteger countGetCr = new AtomicInteger(0);
	
	public JobDriverCasManager(String[] classpath, String crXml, String crCfg) throws JobDriverException {
		initialize(classpath, crXml, crCfg);
	}
	
	public void initialize(String[] classpath, String crXml, String crCfg) throws JobDriverException {
		String location = "initialize";
		try {
			URL[] classLoaderUrls = new URL[classpath.length];
			int i = 0;
			for(String item : classpath) {
				classLoaderUrls[i] = this.getClass().getResource(item);
				i++;
			}
			jdcr = new JobDriverCollectionReader(classLoaderUrls, crXml, crCfg);
		}
		catch(JobDriverException e) {
			logger.error(location, IDuccId.null_id, e);
			throw e;
		}
	}
	
	public MetaCas getMetaCas() throws JobDriverException {
		MetaCas retVal = cacheQueue.poll();
		if(retVal != null) {
			countGet.incrementAndGet();
		}
		else {
			retVal = jdcr.getMetaCas();
			if(retVal != null) {
				countGetCr.incrementAndGet();
				countGet.incrementAndGet();
			}
		}
		return retVal;
	}
	
	public void putMetaCas(MetaCas metaCas) {
		cacheQueue.add(metaCas);
		countPut.incrementAndGet();
	}
	
	public int getTotal() throws JobDriverException {
		return jdcr.getTotal();
	}
	
	public int getGets() {
		return countGet.intValue();
	}
	
	public int getPuts() {
		return countPut.intValue();
	}
	
	public int getGetsCr() {
		return countGetCr.intValue();
	}
}
