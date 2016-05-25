/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
*/
package org.apache.uima.ducc.transport.cmdline;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.uima.ducc.common.utils.Utils;


//@SuppressWarnings("serial")
public abstract class ACommandLine implements ICommandLine {
	/**
	 * increment this sUID value when deleting or modifying a field
	 */
	private static final long serialVersionUID = 1L;
	protected String executable;
//	protected List<String> environment = new ArrayList<String>();
	protected List<String> args = new ArrayList<String>();
	protected String workingDirectory;
	protected String logDirectory;
	protected Map<String, String> environment = new HashMap<String, String>();
	
	public ACommandLine( String executable ) {
		this.executable = executable;
	}
	public String getExecutable() {
		return executable;
	}

	public void addArgument(String arg) {
		args.add(Utils.resolvePlaceholderIfExists(arg, System.getProperties()));
	}
	public List<String> getArguments() {
		return args;
	}
	public void addEnvVar(String key, String value ) {
		environment.put(key, Utils.resolvePlaceholderIfExists(value, System.getProperties()));
	}
	public String getEnvVar(String key) {
		return environment.get(key);
	}
	
	public void addEnvironment(Map<String, String> env) {
		environment.putAll(env);
	}
	public Map<String,String> getEnvironment() {
	  String osArch = System.getProperty("os.arch");
	  // Replace the reserved DUCC variable with the architecture of this node (ppc64 or amd64 or  ...)
	  for (Entry<String, String> ent : environment.entrySet()) {
	    ent.setValue(ent.getValue().replace("${DUCC_OS_ARCH}",  osArch));
	  }
		return environment;
	}
	public void setWorkingDirectory(String workingDirectory) {
		this.workingDirectory = workingDirectory;
	}
	public String getWorkingDirectory() {
		return this.workingDirectory;
	}
	public void setLogDirectory(String logDirectory) {
		this.logDirectory = logDirectory;
	}
	public String getLogDirectory() {
		return this.logDirectory;
	}
	/**
	 * Concatenates multiple arrays into one array of type <A> 
	 * 
	 * @return array of type <A>
	 */
	protected <A> A[] concatAllArrays(A[] first, A[]... next) {
		int totalLength = first.length;
		//	compute the total size of all arrays
		for (A[] array : next) {
			totalLength += array.length;
		}
		A[] result = Arrays.copyOf(first, totalLength);
		int offset = first.length;
		for (A[] array : next) {
			System.arraycopy(array, 0, result, offset, array.length);
			offset += array.length;
		}
		return result;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((args == null) ? 0 : args.hashCode());
		result = prime * result
				+ ((environment == null) ? 0 : environment.hashCode());
		result = prime * result
				+ ((executable == null) ? 0 : executable.hashCode());
		result = prime
				* result
				+ ((workingDirectory == null) ? 0 : workingDirectory.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ACommandLine other = (ACommandLine) obj;
		if (args == null) {
			if (other.args != null)
				return false;
		} else if (!args.equals(other.args))
			return false;
		if (environment == null) {
			if (other.environment != null)
				return false;
		} else if (!environment.equals(other.environment))
			return false;
		if (executable == null) {
			if (other.executable != null)
				return false;
		} else if (!executable.equals(other.executable))
			return false;
		if (workingDirectory == null) {
			if (other.workingDirectory != null)
				return false;
		} else if (!workingDirectory.equals(other.workingDirectory))
			return false;
		return true;
	}

	@Override
	public void addOption(String opt) {
	}
	@Override
	public List<String> getOptions() {
		return null;
	}
}
