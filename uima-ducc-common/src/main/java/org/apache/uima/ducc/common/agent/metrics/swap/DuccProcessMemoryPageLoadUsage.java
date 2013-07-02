package org.apache.uima.ducc.common.agent.metrics.swap;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

public class DuccProcessMemoryPageLoadUsage implements
		ProcessMemoryPageLoadUsage {
	String pid;
	
	public DuccProcessMemoryPageLoadUsage(String pid) {
		this.pid = pid;
	}	
	public long getMajorFaults() throws Exception {
		return collectProcessMajorFaults();
	}
	private long collectProcessMajorFaults() throws Exception {
		String[] command = new String[] {"/bin/ps","-o","maj_flt",pid};

		ProcessBuilder builder = new ProcessBuilder(command);
		Process process = builder.start();
		InputStream is = process.getInputStream();
		InputStreamReader isr = new InputStreamReader(is);
		BufferedReader br = new BufferedReader(isr);
		String line;
		int count = 0;
		String faults = null;
		try {
			while ((line = br.readLine()) != null) {
				// skip the header line
				if (count == 1) {
					faults = line.trim();
					break;
				}
				count++;
			}
		} finally {
			if (is != null) {
				is.close();
			}
			process.destroy();
		}
		process.waitFor();
		if ( faults != null) {
			return Long.parseLong(faults.trim());
		} else {
			return 0;
		}
	}

}
