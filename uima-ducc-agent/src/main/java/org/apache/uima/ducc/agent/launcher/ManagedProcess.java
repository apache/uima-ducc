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

import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import org.apache.uima.ducc.agent.event.ProcessLifecycleObserver;
import org.apache.uima.ducc.agent.launcher.ManagedServiceInfo.ServiceState;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.transport.cmdline.ICommandLine;
import org.apache.uima.ducc.transport.event.common.DuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccStandardInfo;
import org.apache.uima.ducc.transport.event.common.IDuccProcess.ReasonForStoppingProcess;
import org.apache.uima.ducc.transport.event.common.IProcessState.ProcessState;
import org.apache.uima.ducc.transport.event.common.ProcessMemoryAssignment;



public class ManagedProcess implements Process {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	//private static final String JMXPortProperty = "com.sun.management.jmxremote.port"; 
	/*
	 * Process States
	 *                     +------- STARTING
	 *                    /           |
	 *                   /            |
	 *                  V             V
	 *         INITIALIZING ------> FAILED 
	 *               |             /
	 *               |            /
	 *               V           /
	 *              READY ------>
	 *               |
	 *               |
	 *               V
	 *            STOPPED    
	 */
	private ServiceState state= ServiceState.STARTING;
	public static enum ProcessType {
		UIMA_SERVICE{}, 
		APPLICATION{}, 
		LAUNCHER{};
	};


	public static enum StopPriority {
		KILL_9, QUIESCE_AND_STOP
	}
	private volatile boolean destroyed=false;
	private IDuccProcess duccProcess;
	//	Used to kill a process after it reports its PID. 
	private volatile boolean killAfterLaunch=false;
	//private String pid;
	private String processCorrelationId;
	// JMX Port
	private String port = null;
	// Service socket endpoint 
	private String socketEndpoint = null;
	
    private int tgCounter=1;
	//private String owner;
	//	log path for http access
	private String logPath;
	//	absolute path to the process log
	private String absoluteLogPath;

	private String agentLogPath;
	private boolean agentProcess = false;
    private String nodeIp;
    private String nodeName;
    private String description;
    private String parent;
    
	private ProcessType processType;
	private volatile boolean attached;
	private String clientId;
//	private volatile boolean killed;
	private Throwable exceptionStackTrace;
  //	The following are transients to prevent serialization when sending heartbeat updates
	//	to the controller. This class exports its state via AgentManagedProcess interface.
	private transient List<String> command;
	private transient java.lang.Process process;

  protected transient ProcessStreamConsumer stdOutReader;
	protected transient ProcessStreamConsumer stdErrReader;
	//private transient OutputStream processLogStream = null;
	private transient CountDownLatch pidReadyCount = new CountDownLatch(1);

	private ICommandLine commandLine;
	
	private ProcessLifecycleObserver observer;
	
	private Timer initTimer;
	
	// timer used to cleanup UIMA pipeline initializations stats
  private Timer cleanupTimer;

  private DuccId workDuccId;
	
	private IDuccStandardInfo processInfo;
	
	private transient Future<?> future;
	
	private transient volatile boolean isstopping;
	
	private transient volatile boolean forceKill;
	
	private transient DuccLogger logger;
	
	//private long processMemoryAssignment;
	
	private long maxSwapThreshold;
	
	

	private ProcessMemoryAssignment processMemoryAssignment;
	
	public ManagedProcess(IDuccProcess process, ICommandLine commandLine) {
		this(process, commandLine, null, null, new ProcessMemoryAssignment());
	}

	public ManagedProcess(IDuccProcess process, ICommandLine commandLine,boolean agentProcess) {
		this(process, commandLine, null, null,new ProcessMemoryAssignment());
		this.agentProcess = agentProcess;
	}

	public ManagedProcess(IDuccProcess process, ICommandLine commandLine,ProcessLifecycleObserver observer, DuccLogger logger, ProcessMemoryAssignment processMemoryAssignment) {
		this.commandLine = commandLine;
		this.duccProcess = process;
		this.observer = observer;
		this.logger = logger;
		this.processMemoryAssignment = processMemoryAssignment;
	}
	public ManagedProcess( java.lang.Process process, boolean agentProcess, String correlationId ) {
		this.process = process;
		this.agentProcess = agentProcess;
	}
	public ManagedProcess( java.lang.Process process, boolean agentProcess ) {
		this(process, agentProcess, null );
	}
	public boolean doKill() {
		return forceKill;
	}
	public void kill() {
		forceKill = true;
	}
	public void setStopping() {
		isstopping = true;
	}
	public boolean isStopping() {
		return isstopping;
	}
	public void setWorkDuccId( DuccId workDuccId ) {
		this.workDuccId = workDuccId;
	}
	public DuccId getWorkDuccId() {
		return this.workDuccId;
	}
	public DuccId getDuccId() {
		return duccProcess.getDuccId();
	}
	public void setParent( String parent) {
		this.parent = parent;
	}
	public String getParent() {
		return parent;
	}
	
	public void setClientId(String clientId){
		this.clientId = clientId;
	}
    public String getClientId() {
    	return clientId;
    }
	public boolean isAttached() {
		return attached;
	}

	public void setAttached() {
		this.attached = true;
	}
	
	public String getNodeName() {
		return nodeName;
	}

	public void setNodeName(String nodeName) {
		this.nodeName = nodeName;
	}
	
	public String getNodeIp() {
		return nodeIp;
	}

	public void setNodeIp(String nodeIp) {
		this.nodeIp = nodeIp;
	}

	/**
	 * Return current state of this object.
	 * 
	 * @return
	 */
	public Process getInstance() {
		return this;
	}
	public ProcessType getProcessType() {
		return processType;
	}
	public void setProcessType(ProcessType processType) {
		this.processType = processType;
	}
	public void setLogStream(OutputStream os ) {
		//processLogStream = os;
	}
	public boolean isAgentProcess() {
		return agentProcess;
	}
	/**
	 * @param uimaLogPath the uimaLogPath to set
	 */
	public void setLogPath(String logPath) {
		this.logPath = logPath;
	}

	/**
	 * @return the stdErrLogPath
	 */
	public String getLogPath() {
		return logPath;
	}
	public void setAgentLogPath(String logPath) {
		this.agentLogPath = logPath;
	}

	/**
	 * @return the stdErrLogPath
	 */
	public String getAgentLogPath() {
		return agentLogPath;
	}
	/**
	 * @return the owner
	 */
	public String getOwner() {
		if ( processInfo != null ) {
			return processInfo.getUser();
		}
		return null;
	}


	public void setOSProcess(java.lang.Process process ) {
		this.process = process;
	}
	private void log( String method, String message ) {
	  if ( logger != null ) {
	    logger.info(method, null, message);
	  }
	}
	public void drainProcessStreams(java.lang.Process process , DuccLogger logger, PrintStream pStream, boolean isKillCmd ) { // InputStream outputStream, InputStream errorStream) {

		//	Create dedicated Thread Group for the process stream consumer threads
		ThreadGroup group = new ThreadGroup("AgentDeployer" + tgCounter++);
		//	Fetch stdin from the deployed process
		InputStream stdin = process.getInputStream();
		//	Fetch stderr from the deployed process
		InputStream stderr = process.getErrorStream();
		//	Create dedicated thread to consume std output stream from the process 
		stdOutReader = new ProcessStreamConsumer(logger, group, "StdOutputReader", stdin, pStream);
		//	Create dedicated thread to consume std error stream from the process 
		stdErrReader = new ProcessStreamConsumer(logger,  group, "StdErrorReader", stderr, pStream);
		
		//	Start both stream consumer threads
		stdOutReader.start();
		stdErrReader.start();
		//	block until the process is terminated or the agent terminates
		boolean finished = false;
		while (!finished) {
			try {
				process.waitFor();
			} catch (InterruptedException e) {
			}
			try {
				process.exitValue();
				finished = true;
			} catch (IllegalThreadStateException e) {
			}
		}
		try {
		  // wait for stdout and stderr reader threads to finish. Join for max of 2 secs 
      // The process has exited and in theory the join should return quickly.
		  // We do the join to make sure that the streams are drained so that we
		  // can get a reason for failure if there was a problem launching the process.
		  stdOutReader.join(2000);
	    stdErrReader.join(2000);
		} catch( InterruptedException ie) {
		  log("ManagedProcess.drainProcessStreams","Interrupted While Awaiting Termination of StdOutReader and StdErrReader Threads");
		}
		
		String reason = 
		        getDuccProcess().getReasonForStoppingProcess();
		
		ProcessState pstate = getDuccProcess().getProcessState();

		if ( isKillCmd || 
		        // if the process is to be killed due to init problems, set the state to Stopped
		        ( reason != null &&
		            (
		               reason.equals(ReasonForStoppingProcess.FailedInitialization.toString() ) || 
		               reason.equals(ReasonForStoppingProcess.InitializationTimeout.toString() )
		            ) 
		       )
		   ) {
		  getDuccProcess().setProcessState(ProcessState.Stopped);
		  
		} else {
	    // Process has terminated. Determine why the process terminated.
      log("ManagedProcess.drainProcessStreams", "Ducc Process with PID:"+getPid() +" Terminated while in "+pstate+" State");
	    
	    //  true if agent killed the process. Process either exceeded memory use or
	    //  the PM state notifications stopped coming in.
	    if ( doKill() ) {
        // Agent killed the process due to timeout waiting for OR state
        pstate = ProcessState.Killed;
	    } else {
	      // check if the process died due to an external cause. If there was the
	      // case isstopping = false. The isstopping=true iff the Agent initiated
	      // process stop because the process was deallocated
	      if ( !isstopping) {
	        //  unexpected process death. Killed by the OS or admin user with kill -9
	        pstate = ProcessState.Failed;
	        // fetch errors from stderr stream. If the process failed to start due to misconfiguration
	        // the reason for failure would be provided by the OS (wrong user id, bad directory,etc) 
	        String errors = stdErrReader.getDataFromStream();
	        if ( errors.trim().length() > 0 ) {
	          getDuccProcess().setReasonForStoppingProcess(errors.trim());
	        } else {
	          getDuccProcess().setReasonForStoppingProcess(ReasonForStoppingProcess.Croaked.toString());
	        }
	      } else {
	        // if the process was stopped due InitializationFailure or InitializationTimeout
	        // the following method will be a noop.
          addReasonForStopping(getDuccProcess(),ReasonForStoppingProcess.Deallocated.toString());
	        pstate = ProcessState.Stopped;
	      }
	      notifyProcessObserver(pstate);
	      log("ManagedProcess.drainProcessStreams","************ Remote Process PID:"+getPid()+" Terminated ***************");
	    }
		}
	}
	private void addReasonForStopping(IDuccProcess process, String reason) {
	  if ( getDuccProcess().getReasonForStoppingProcess() == null || getDuccProcess().getReasonForStoppingProcess().trim().length() == 0 ) {
	    getDuccProcess().setReasonForStoppingProcess(reason);
	  }
	}
	/**
	 * @return the state
	 */
	public ServiceState getStatus() {
		return state; 
	}

	/**
	 * @return the pid
	 */
	public String getPid() {
		return duccProcess.getPID();
	}
	/**
	 * @param pid the pid to set
	 */
	public void setPid(String pid) {
		//this.pid = pid;
		duccProcess.setPID(pid);
		pidReadyCount.countDown();
	}
	public void awaitPid() {
		try {
			pidReadyCount.await();
		} catch( Exception e) {
		}
	}
	public void releasePidLatch() {
		pidReadyCount.countDown();
	}
	/**
	 * @return the command
	 */
	public List<String> getCommand() {
		return command;
	}
	/**
	 * @param command the command to set
	 */
	/**
	 * @param command the command to set
	 */
	public void setCommand(List<String> commandToRun) {
		this.command = commandToRun;
	}
	
	/**
	 * @return the processCorrelationId
	 */
	public String getProcessId() {
		return processCorrelationId;
	}
	/**
	 * @param processCorrelationId the processCorrelationId to set
	 */
	public void setProcessId(String processId) {
		this.processCorrelationId = processId;
	}
	/**
	 * @return the jmxPort
	 */
	public String getPort() {
		return port;
	}
	/**
	 * @param jmxPort the jmxPort to set
	 */
	public void setPort(String port) {
		this.port = port;
	}
	public void terminateRemoteProcess() throws Exception {
		System.out.println("Stopping Remote Managed Process via JMX");
		//	Using JMX instruct the service to terminate	
//		if ( processMBean != null ) {
//			processMBean.getManagedServiceMBean().terminate();
//		}
	}
	public void failed() {
//		setStatus(ServiceState.FAILED);
		cleanup();
//		managedProcessInfo.setState(state);
//		if ( getEndDate() == null ) {
//			setEndDate(new DateTime());
//		}
	}
	private void cleanup() {
		if ( process != null ) {
			process.destroy();
		}
		destroyed = true;
		
	}
	public void stop() {
//	  if ( getStatus() != ServiceState.FAILED ) {
//	    setStatus(ServiceState.STOPPED);
//	  }
		cleanup();
//		if ( getEndDate() == null ) {
//			setEndDate(new DateTime());
//		}
//		if ( process != null ) {
//			process.destroy();
//		}
//		destroyed = true;
	}
	public void waitFor() throws InterruptedException {
		process.waitFor();
	}
	
    public boolean isDestroyed() {
    	return destroyed;
    }
	public String getAbsoluteLogPath() {
		return absoluteLogPath;
	}
	public void setAbsoluteLogPath(String absoluteLogPath) {
		this.absoluteLogPath = absoluteLogPath;
	}
	public String getDescription() {
		return description;
	}
	public void setDescription(String description) {
		this.description = description;
	}
	 public Throwable getExceptionStackTrace() {
	    return exceptionStackTrace;
	  }
	  public void setExceptionStackTrace(Throwable exceptionStackTrace) {
	    this.exceptionStackTrace = exceptionStackTrace;
	  }
	/**
	 * @return the processSpecification
	 */
	public ICommandLine getCommandLine() {
		return commandLine;
	}
	/**
	 * @return the duccProcess
	 */
	public IDuccProcess getDuccProcess() {
		return duccProcess;
	}
	public void notifyProcessObserver(ProcessState state) {
		if ( observer != null && getDuccProcess() != null ) {
			if ( ProcessState.InitializationTimeout.equals(state)) {
        observer.onJPInitTimeout(getDuccProcess());
			} else {
	      getDuccProcess().setProcessState(state);
	      observer.onProcessExit(getDuccProcess());
			}
		}
	}
	public void startInitializationTimer() {
		initTimer = new Timer();
		long timeout = 7200*1000;  // default timeout after 2 hours of initialization
		try {
			String str_timeout;
			if ( (str_timeout = System.getProperty("ducc.agent.launcher.process.init.timeout")) != null ) {
				timeout = Long.parseLong(str_timeout);
			}
		} catch( NumberFormatException e) {
			
		}
		initTimer.schedule( new InitializationTask(), timeout);  // timeout after 2 hours of initialization
	}

	/**
	 * Stops service initialization timer and starts a new timer that will cleanup
	 * UIMA pipeline initialization stats. These states are only needed during the
	 * initialization of a service.
	 */
	public void stopInitializationTimer() {
		if ( initTimer != null ) {
			initTimer.cancel();
	    long timeout = 60000;  // default timeout after 60 seconds
			if ( getDuccProcess().getUimaPipelineComponents() != null && getDuccProcess().getUimaPipelineComponents().size() > 0) {
			  cleanupTimer = new Timer();
		    try {
		      String inv_publish_rate;
		      if ( (inv_publish_rate = System.getProperty("ducc.agent.node.inventory.publish.rate")) != null ) {
		        timeout = Long.parseLong(inv_publish_rate)*4;  // allow sufficient time to publish init stats
		      }
		    } catch( NumberFormatException e) {
		    }
		    cleanupTimer.schedule( new CleanupTask(), timeout);  // timeout and remove UIMA pipeline stats
			}
		}
	}
	private class InitializationTask extends TimerTask {
		public void run() {
			initTimer.cancel();
      notifyProcessObserver(ProcessState.InitializationTimeout);
		}
	}
  private class CleanupTask extends TimerTask {
    public void run() {
      cleanupTimer.cancel();
      ((DuccProcess)getDuccProcess()).setUimaPipelineComponents(null);
    }
  }

  public IDuccStandardInfo getProcessInfo() {
		return processInfo;
	}

	public void setProcessInfo(IDuccStandardInfo processInfo) {
		this.processInfo = processInfo;
	}

	public boolean killAfterLaunch() {
		return killAfterLaunch;
	}

	public void killAfterLaunch(boolean killAfterLaunch) {
		this.killAfterLaunch = killAfterLaunch;
	}

	public Future<?> getFuture() {
		return future;
	}

	public void setFuture(Future<?> future) {
		this.future = future;
	}

  public ProcessMemoryAssignment getProcessMemoryAssignment() {
    return processMemoryAssignment;
  }

  public String getSocketEndpoint() {
    return socketEndpoint;
  }

  public void setSocketEndpoint(String socketEndpoint) {
    this.socketEndpoint = socketEndpoint;
  }
  
  public long getMaxSwapThreshold() {
		return maxSwapThreshold;
	}

  public void setMaxSwapThreshold(long maxSwapThreshold) {
		this.maxSwapThreshold = maxSwapThreshold;
	}
}
