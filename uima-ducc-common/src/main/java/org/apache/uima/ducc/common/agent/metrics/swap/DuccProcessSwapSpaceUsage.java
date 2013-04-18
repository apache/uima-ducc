package org.apache.uima.ducc.common.agent.metrics.swap;

import java.io.BufferedReader;
import java.io.InputStreamReader;


import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;

public class DuccProcessSwapSpaceUsage implements ProcessSwapSpaceUsage {
	String pid=null;
	String execScript=null;
	DuccLogger logger=null;
	String[] command;
	
	public DuccProcessSwapSpaceUsage( String pid, String owner, String execScript, DuccLogger logger) {
		this.pid = pid;
		this.execScript = execScript;
		this.logger = logger;
	    String c_launcher_path = 
	            Utils.resolvePlaceholderIfExists(
	                    System.getProperty("ducc.agent.launcher.ducc_spawn_path"),System.getProperties());
	    command = new String[] { c_launcher_path,
	              "-u", owner, "--", execScript, pid }; 
	}
	public long getSwapUsage() {
		long swapusage=0;
		if ( pid != null && execScript != null ) {
			InputStreamReader in = null;
			try {
				ProcessBuilder pb = new ProcessBuilder();
				//String[] command = {execScript,pid};
				pb.command(command); //command);
				String cmd = "";
				for( String c : command) {
					cmd += " "+ c;
				}
				//logger.info("------------ getSwapUsage-", null, cmd);
				pb.redirectErrorStream(true);
				Process swapCollectorProcess = pb.start();
				in = new InputStreamReader(swapCollectorProcess.getInputStream());
				BufferedReader reader = new BufferedReader(in);
				String line=null;
				boolean skip = true;
				while ((line = reader.readLine()) != null) {
					try {
						if ( line.startsWith("1001")) {
							skip = false;
							continue;
						}
						if (!skip) {
							swapusage = Long.parseLong(line.trim());
							logger.info("getSwapUsage-",null, "PID:"+pid+" Swap Usage:"+line);
						}
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
