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
import java.util.List;

public class JavaCommandLine extends ACommandLine {
	
	private static final long serialVersionUID = 1L;
	
	private String className;
	private String classpath;
	protected List<String> options = new ArrayList<String>();

	public JavaCommandLine(String executable) {
		super(executable);
	}
    public void addOption(String option ) {
        if (!options.contains(option)) {
            options.add(option);
        }
	}
	public List<String> getOptions() {
		return options;
	}
	public String[] getCommandLine() {
		String[] os = new String[options.size()];
		String[] args = new String[super.args.size()];
		if ( super.args == null || super.args.size() == 0 ) {
			return super.concatAllArrays(
					options.toArray(os), new String[] { "-classpath",classpath,
					className });
		} else {
			return super.concatAllArrays(
					options.toArray(os), new String[] { "-classpath",classpath,
					className }, super.args.toArray(args));
		}
	}

	public String getCommand() {
		String retVal = "";
		for(String part : getCommandLine()) {
			retVal += " "+part;
		}
		return retVal;
	}
	
	/**
	 * @return the className
	 */
	public String getClassName() {
		return className;
	}

	/**
	 * @param className
	 *            the className to set
	 */
	public void setClassName(String className) {
		this.className = className;
	}

	/**
	 * @return the classpath
	 */
	public String getClasspath() {
		return classpath;
	}

	/**
	 * @param classpath
	 *            the classpath to set
	 */
	public void setClasspath(String classpath) {
		this.classpath = classpath;
	}
	public void prependToClasspath(String cp) {
		if ( !cp.trim().endsWith(System.getProperty("path.separator"))) {
			cp = cp.trim()+System.getProperty("path.separator");
		}
		this.classpath = cp+this.classpath;
	}
	public static void main(String[] args) {
		try {
			JavaCommandLine cmdLine = new JavaCommandLine("/share/jdk1.6/bin/java");
			cmdLine.addOption("-Xmx=200M");
			cmdLine.addOption("-Xms=100M");
			cmdLine.addOption("-DUIMA_HOME=$HOME/uima");
			cmdLine.setClasspath("$UIMA_HOME/lib/*;$UIMA_HOME/lib/optional/*");
			cmdLine.setClassName("org.apache.uima.ducc.agent.deploy.ManagedUimaService");
			cmdLine.addArgument("/tmp/UimaASDeploymentDescriptor.xml");
			for(String part : cmdLine.getCommandLine()) {
				System.out.println("-- "+part);
			}
		} catch( Exception e) {
			e.printStackTrace();
		}
	}
}
