package org.apache.uima.ducc.common.agent.metrics.swap;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.uima.ducc.common.utils.DuccLogger;

public class DuccProcessSwapSpaceUsage implements ProcessSwapSpaceUsage {
	String pid=null;
	String execScript=null;
	DuccLogger logger=null;
	
	public DuccProcessSwapSpaceUsage( String pid, String execScript, DuccLogger logger) {
		this.pid = pid;
		this.execScript = execScript;
		this.logger = logger;
	}
	public long getSwapUsage() {
		long swapusage=0;
		if ( pid != null && execScript != null ) {
			InputStreamReader in = null;
			try {
				ProcessBuilder pb = new ProcessBuilder();
				String[] command = {execScript,pid};
				pb.command(command);
				pb.redirectErrorStream(true);
				Process swapCollectorProcess = pb.start();
				in = new InputStreamReader(swapCollectorProcess.getInputStream());
				BufferedReader reader = new BufferedReader(in);
				String line=null;
				
				while ((line = reader.readLine()) != null && line.trim().length() > 0 ) {
					try {
						swapusage = Long.parseLong(line.trim());
					} catch( NumberFormatException e) {
						logger.error("getSwapUsage", null, line);
					}
				}
			} catch( Exception e) {
				logger.error("getSwapUsage", null, e);
			} finally {
				if ( in != null ) {
					try {
						in.close();	
					} catch( Exception e) {
						logger.error("getSwapUsage", null, e);
					}
					
				}
			}
		}
		return swapusage;
	}
	
}
