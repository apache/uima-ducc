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
package org.apache.uima.ducc.common.launcher.ssh;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;

public class DuccRemoteLauncher {
	public static final String FileSeparator = System.getProperty("file.separator");

	//private final Logger logger = Logger.getLogger(DuccRemoteLauncher.class.getName());
	private String sshUser;
	private JSch jsch = new JSch();
	private Executor executor = Executors.newCachedThreadPool();
	private OutputStream outputStream;
	public DuccRemoteLauncher() {
		
	}
	public DuccRemoteLauncher(String sshUser, String sshIdentityLocation, OutputStream out) throws Exception {
		this.outputStream = out;
		this.sshUser = sshUser;
		jsch.addIdentity(sshIdentityLocation);
	}
	
	public Future<ProcessCompletionResult> execute(final String host, final String command, final ProcessCompletionCallback callback) {
		ExecutableTask task2Run = 
				new ExecutableTask(this,callback,host,command);
		FutureTask<ProcessCompletionResult> future = 
				new FutureTask<ProcessCompletionResult>(task2Run);
		executor.execute(future);
		return future;
	}
		
	/**
	 * Executes a process on a remote node using provided command line.
	 * 
	 * @param host - node where to launch the process
	 * @param command - command line to launch the process
	 * @return
	 */
	private ProcessCompletionResult runTask(String host, String command) {
		int exitCode = 0;
		ProcessCompletionResult result = null;
		// all errors will be captured in the error stream
		ByteArrayOutputStream errorStream = new ByteArrayOutputStream();
		ChannelExec channel = null;
		try {
			Session session = createSSHSession(host);
			channel = (ChannelExec) session.openChannel("exec");
			channel.setCommand(command);
			channel.setInputStream(null);
			((ChannelExec)channel).setErrStream(errorStream);
			InputStream in=channel.getInputStream();
			//	connect actually execs the process on a remote node
			channel.connect();
			//  synchronously consume the stdout. If no stdout is available
			//  this method exits.
			consumeProcessStream(host, in, outputStream, channel);
			channel.disconnect();
			if ( channel.getExitStatus() != 0 || errorStream.size() > 0 ) {
				result =  new ProcessCompletionResult(channel.getExitStatus(), host, command, new String(errorStream.toByteArray()));
			} else {
				result =  new ProcessCompletionResult(channel.getExitStatus(), host, command);
			}
		} catch (Throwable e) {
			if ( channel == null ) {
				exitCode = -1;
			} else {
				exitCode = channel.getExitStatus();
			}
			try {
				outputStream.write(errorStream.toByteArray());
			} catch( Exception ex) {
				ex.printStackTrace();
			}
			result =  new ProcessCompletionResult(exitCode, host,command, new String(errorStream.toByteArray()), e);
		}	
		return result;
	}
	/**
	 * Consumes remote process stdout one line at a time and writing it out to provided
	 * output stream prepending host name to each line. 
	 * 
	 * @param host - host from which the stream is consumed
	 * @param is - remote process stdout stream
	 * @param os - where to  write the stream
	 * @param channel - ssh channel to remote process
	 * @return - exit code
	 * 
	 * @throws Exception
	 */
	private int consumeProcessStream(final String host, final InputStream is, final OutputStream os, final ChannelExec channel ) throws Exception {
		int exitCode = -1;
		InputStreamReader ins = new InputStreamReader(is);
		final BufferedReader reader = new BufferedReader(ins);
		String line;
		try {
    		while ((line = reader.readLine()) != null) {
    			os.write((host+" : "+line+'\n').getBytes());
    			os.flush();
    			if(channel.isClosed()) {
    				break;
    			}
    		}	
    	    // if channel is closed, the process must have exited
    	    if(channel.isClosed()) {
    	    	exitCode = channel.getExitStatus();
    	    }
			
		} catch(Exception e) {
			e.printStackTrace();
		
		}
		return exitCode;
	}
	private Session createSSHSession(String host) throws JSchException {
		Session newSession = jsch.getSession(sshUser, host, 22);
		newSession.setTimeout(500);
		Properties props = new Properties();
		props.put("StrictHostKeyChecking", "no");
		newSession.setConfig(props);
		newSession.connect();
		return newSession;
	}

	
	public static class ProcessCompletionResult {
		public String stderr;
		public int exitCode;
		public String host;
		public Throwable e;
		public String command;
		
		public ProcessCompletionResult( int exitCode, String host, String command) {
			this(exitCode, host, command, null, null);
		}
		public ProcessCompletionResult( int exitCode, String host, String command, String errors) {
			this(exitCode, host, command, errors, null);
		}
		public ProcessCompletionResult( int exitCode, String host, String command, String errors, Throwable e) {
			this.exitCode = exitCode;
			this.host = host;
			this.stderr = errors;
			this.e = e;
			this.command = command;
		}
	}
	public static interface ProcessCompletionCallback {
		public void completed(ProcessCompletionResult result); 
	}

	public static class ExecutableTask implements Callable<ProcessCompletionResult> {
		private ProcessCompletionCallback callback;
		private String host;
		private String command;
		private DuccRemoteLauncher launcher;
		
		public ExecutableTask(DuccRemoteLauncher launcher, final DuccRemoteLauncher.ProcessCompletionCallback callback, String host, String command) {
			this.callback = callback;
			this.host = host;
			this.command = command;
			this.launcher = launcher;
		}
		public ProcessCompletionResult call() throws Exception {
			ProcessCompletionResult result = launcher.runTask(host.toLowerCase(), command);
			if (callback != null) {
				callback.completed(result);
			}
			return result;
		}
		
	}
}
