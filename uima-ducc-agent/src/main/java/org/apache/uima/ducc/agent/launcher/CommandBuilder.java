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
package org.apache.uima.ducc.agent.launcher;

import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.transport.cmdline.ICommandLine;
import org.apache.uima.internal.util.ArrayUtils;

public class CommandBuilder {

	private static final String ducclingPath = Utils.resolvePlaceholderIfExists(
			System.getProperty("ducc.agent.launcher.ducc_spawn_path"),
			System.getProperties());
	
	public CommandBuilder() {
		
	}
	private static boolean useDuccling(ManagedProcess process) {
		if ( process.isAgentProcess() || Utils.isWindows()) {
			return false;
		}
		// On non-windows check if we should spawn the process via ducc_ling
		String useDuccling = System
				.getProperty("ducc.agent.launcher.use.ducc_spawn");
		return ("true".equalsIgnoreCase(useDuccling) ? true : false );
	}

	public static String[] deployableStopCommand(ICommandLine cmdLine, ManagedProcess process) {
		String[] cmd;
		// Duccling, with no logging, always run by ducc, no need for
		// workingdir
		String[] ducclingNolog = new String[] { ducclingPath, "-u",
				process.getOwner(), "--" };
		
		if ( useDuccling(process) ) {
			cmd = (String[]) ArrayUtils.combine( ducclingNolog,
					(String[]) ArrayUtils.combine(new String[] { cmdLine.getExecutable() },
					cmdLine.getCommandLine()));
		} else {
			cmd = (String[]) ArrayUtils.combine(
					new String[] { cmdLine.getExecutable() },
					cmdLine.getCommandLine());
		}
		return cmd;
	}
}
