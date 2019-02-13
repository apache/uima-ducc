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
	/**
     * Assign the generated ID so will (hopefully) match the one assigned when serialized
     */
    private static final long serialVersionUID = 7377333447388157462L;
    private String className;
	private String classpath;
	protected List<String> options = new ArrayList<String>();

	public JavaCommandLine(String executable) {
		super(executable);
	}

	/*
	 * Make a shallow copy of everything except the options as they are modified by DuccCommandExecutor
	 */
	public JavaCommandLine copy() {
	  JavaCommandLine jcl = new JavaCommandLine(executable);
	  jcl.className = className;
	  jcl.classpath = classpath;
	  jcl.logDirectory = logDirectory;
	  jcl.workingDirectory = workingDirectory;
	  jcl.args = args;
	  jcl.environment = environment;
	  jcl.options = new ArrayList<String>(getOptions());
	  return jcl;
	}

    public void addOption(String option ) {
        if (!options.contains(option)) {
            options.add(option);
        }
	}
    public void replaceOption(String key, String value) {
    	List<String> newOptions = new ArrayList<>();
    	boolean modified = false;
    	for( String option : getOptions() ) {
    		String[] keyValue = option.split("=");
    		if ( keyValue[0].equals(key)) {
    			newOptions.add(key+"="+value);
    			modified=true;
    		} else {
    			newOptions.add(option);
    		}
    		
    	}
    	if ( modified ) {
    		options.clear();
    		options = newOptions;
    	}
    }
	public List<String> getOptions() {
		return options;
	}
	public String[] getCommandLine() {
		String[] os = new String[options.size()];
		String[] result;
		if ( args == null || args.size() == 0 ) {
      result = concatAllArrays(options.toArray(os), new String[] { "-classpath", classpath, className });
		} else {
      String[] arguments = new String[args.size()];
      result = concatAllArrays(options.toArray(os), new String[] { "-classpath", classpath, className },
              args.toArray(arguments));
		}
		return result;
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
	 * @param classNameoptions
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
