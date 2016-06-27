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

public class NonJavaCommandLine extends ACommandLine {
	/**
     * Assign the generated ID so will (hopefully) match the one assigned when serialized 
     */
    private static final long serialVersionUID = 4905426747918479474L;
    public NonJavaCommandLine(String executable) {
		super(executable);
	}
	public String[] getCommandLine() {
		String[] args = new String[super.args.size()];
		return super.args.toArray(args);
	}
	public String getCommandLineString() {
		// TODO Auto-generated method stub
		return null;
	}
	public static void main(String[] args) {
		try {
			NonJavaCommandLine cmdLine = new NonJavaCommandLine("someExecutable.exe");
			cmdLine.addArgument("arg1");
			cmdLine.addArgument("arg2");
			cmdLine.addArgument("arg3");
			for(String part : cmdLine.getCommandLine()) {
				System.out.println("-- "+part);
			}
		} catch( Exception e) {
			e.printStackTrace();
		}
	}
	
}
