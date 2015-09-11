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
package org.apache.uima.ducc.pm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.camel.CamelContext;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties;
import org.apache.uima.ducc.common.boot.DuccDaemonRuntimeProperties.DaemonName;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.pm.helper.DuccWorkHelper;
import org.apache.uima.ducc.transport.cmdline.ICommandLine;
import org.apache.uima.ducc.transport.dispatcher.DuccEventDispatcher;
import org.apache.uima.ducc.transport.event.DuccEvent;
import org.apache.uima.ducc.transport.event.DuccJobsStateEvent;
import org.apache.uima.ducc.transport.event.PmStateDuccEvent;
import org.apache.uima.ducc.transport.event.common.DuccJobDeployment;
import org.apache.uima.ducc.transport.event.common.DuccUserReservation;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkPop;
import org.apache.uima.ducc.transport.event.common.DuccWorkPopDriver;
import org.apache.uima.ducc.transport.event.common.DuccWorkReservation;
import org.apache.uima.ducc.transport.event.common.IDuccJobDeployment;
import org.apache.uima.ducc.transport.event.common.IDuccProcess;
import org.apache.uima.ducc.transport.event.common.IDuccReservationMap;
import org.apache.uima.ducc.transport.event.common.IDuccUnits.MemoryUnits;
import org.apache.uima.ducc.transport.event.common.IDuccWork;
import org.apache.uima.ducc.transport.event.common.IDuccWorkExecutable;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.transport.event.common.ProcessMemoryAssignment;

/**
 * The ProcessManager's main role is to receive Orchestrator updates, trim received state and
 * publish a new state to the agents. Trimming is done to remove state that is irrelevant to
 * the agents. 
 */
public class ProcessManagerComponent extends AbstractDuccComponent 
implements ProcessManager {
	private static final String[] jobHeaderArray=
		{"DuccId","ProcessType","PID","ProcessState","ResourceState","NodeIP","NodeName","DeallocationType","JMX Url"};
	private static final String jobHeaderFormat =
		"%1$-15s|%2$-20s|%3$-10s|%4$-15s|%5$-15s|%6$-13s|%7$-45s|%8$-25s|%9$-45s";
	
	private static String header;
	private static String tbl=String.format("%1$-158s"," ").replace(" ", "-");
	public static DuccLogger logger = new DuccLogger(ProcessManagerComponent.class, DuccComponent);
	
	private static DuccWorkHelper dwHelper = null;
	
	//	Dispatch component used to send messages to remote Agents
	private DuccEventDispatcher eventDispatcher;
  private int shareQuantum;
  private int fudgeFactor = 5; // default 5%
  
	public ProcessManagerComponent(CamelContext context, DuccEventDispatcher eventDispatcher) {
		super("ProcessManager",context);
		this.eventDispatcher = eventDispatcher;
		if ( System.getProperty("ducc.rm.share.quantum") != null && System.getProperty("ducc.rm.share.quantum").trim().length() > 0 ) {
	    shareQuantum = Integer.parseInt(System.getProperty("ducc.rm.share.quantum").trim());
		}
    if ( System.getProperty("ducc.agent.share.size.fudge.factor") != null &&
         System.getProperty("ducc.agent.share.size.fudge.factor").trim().length() > 0) {
      fudgeFactor = Integer.parseInt(System.getProperty("ducc.agent.share.size.fudge.factor").trim());
    }
		
		header = 
				String.format(jobHeaderFormat,jobHeaderArray[0],jobHeaderArray[1],jobHeaderArray[2],
						   jobHeaderArray[3],jobHeaderArray[4],jobHeaderArray[5],jobHeaderArray[6],
						   jobHeaderArray[7],jobHeaderArray[8]+"\n");
		dwHelper = new DuccWorkHelper();
	}
	public void start(DuccService service) throws Exception {
		super.start(service, null);
		DuccDaemonRuntimeProperties.getInstance().boot(DaemonName.ProcessManager,getProcessJmxUrl());
		logger.info("start", null, "--PM started - jmx URL:"+super.getProcessJmxUrl());
		
	}	
	public DuccLogger getLogger() {
	    return logger;
	  }
	
	/* New Code */
	
	private long normalizeMemory(String processMemoryAssignment, MemoryUnits units) {
		 //  Get user defined memory assignment for the JP
	    long normalizedProcessMemoryRequirements =
	            Long.parseLong(processMemoryAssignment);
	    // Normalize memory requirements for JPs into Gigs 
	    if ( units.equals(MemoryUnits.KB ) ) {
	      normalizedProcessMemoryRequirements = (int)normalizedProcessMemoryRequirements/(1024*1024);
	    } else if ( units.equals(MemoryUnits.MB ) ) {
	      normalizedProcessMemoryRequirements = (int)normalizedProcessMemoryRequirements/1024;
	    } else if ( units.equals(MemoryUnits.GB ) ) {
	      //  already normalized
	    } else if ( units.equals(MemoryUnits.TB ) ) {
	      normalizedProcessMemoryRequirements = (int)normalizedProcessMemoryRequirements*1024;
	    }
	    return normalizedProcessMemoryRequirements;
	}
	private int getShares(long normalizedProcessMemoryRequirements ) {
	    int shares = (int)normalizedProcessMemoryRequirements/shareQuantum;  // get number of shares
	    if ( (normalizedProcessMemoryRequirements % shareQuantum) > 0 ) shares++; // ciel
	    return shares;
	}

	private String getCmdLine(ICommandLine iCommandLine) {
		StringBuffer sb = new StringBuffer();
		if(iCommandLine != null) {
			String[] commandLine = iCommandLine.getCommandLine();
			if(commandLine != null) {
				for(String item : commandLine) {
					sb.append(item);
					sb.append(" ");
				}
			}
		}
		return sb.toString();
	}

	public void dispatchStateUpdateToAgents(Map<DuccId, IDuccWork> workMap, long sequence) {
    String methodName="dispatchStateUpdateToAgents";
	  try {
	    dumpState(workMap);
	    // Create a job list which the PM will dispatch to agents
	    List<IDuccJobDeployment> jobDeploymentList = new ArrayList<IDuccJobDeployment>();
	    List<DuccUserReservation> reservationList = new ArrayList<DuccUserReservation>();

	    //  Populate job list with job data that agents need. Don't copy data the agent doesnt
	    //  care about.
	    for( Entry<DuccId, IDuccWork> entry : workMap.entrySet() ) {
	      if ( entry.getValue() instanceof DuccWorkJob ) {
	        DuccWorkJob dcj = (DuccWorkJob)entry.getValue();
	        //  Create process list for each job
	        List<IDuccProcess> jobProcessList = new ArrayList<IDuccProcess>();
	        
	        long normalizedProcessMemoryRequirements = normalizeMemory(dcj.getSchedulingInfo().getMemorySizeRequested(),dcj.getSchedulingInfo().getMemoryUnits());
	        int shares = getShares(normalizedProcessMemoryRequirements);
	        long processAdjustedMemorySize = shares * shareQuantum * 1024;  
	        ProcessMemoryAssignment pma = new ProcessMemoryAssignment();
	        pma.setShares(shares);
	        pma.setNormalizedMemoryInMBs(processAdjustedMemorySize);
	        
	    
	        //  Copy job processes 
	        for( Entry<DuccId,IDuccProcess> jpProcess : dcj.getProcessMap().getMap().entrySet()) {
	          jobProcessList.add(jpProcess.getValue());
	        }
	        
	        
/*	        
	        if ( dcj.getUimaDeployableConfiguration() instanceof DuccUimaDeploymentDescriptor ) {
	          //  Add deployment UIMA AS deployment descriptor path
	          ((JavaCommandLine)dcj.getCommandLine()).
	            addArgument(((DuccUimaDeploymentDescriptor)dcj.getUimaDeployableConfiguration()).getDeploymentDescriptorPath());
	        }
*/	        
	        //  add fudge factor (5% default)  to adjust memory computed above 
	        processAdjustedMemorySize += (processAdjustedMemorySize * ((double)fudgeFactor/100));
	        pma.setMaxMemoryWithFudge(processAdjustedMemorySize);
	        
	        logger.debug(methodName,dcj.getDuccId(),"--------------- User Requested Memory For Process:"+dcj.getSchedulingInfo().getMemorySizeRequested()+dcj.getSchedulingInfo().getMemoryUnits()+" PM Calculated Memory Assignment of:"+processAdjustedMemorySize);
	        
	        ICommandLine driverCmdLine = null;
	        ICommandLine processCmdLine = null;
	        IDuccProcess driverProcess = null;
	        IDuccWork dw = null;
	        
	        switch(dcj.getDuccType()) {
	        case Job:
	          logger.debug(methodName, dcj.getDuccId(), "case: Job");
	          dw = dwHelper.fetch(dcj.getDuccId());
	          IDuccWorkJob job = (IDuccWorkJob) dw;
	          DuccWorkPopDriver driver = job.getDriver();
			  if(driver != null) {
				  driverCmdLine = driver.getCommandLine();
				  driverProcess = driver.getProcessMap().entrySet().iterator().next().getValue();
			  }
	          processCmdLine = job.getCommandLine();
	          break;
	        case Service:
	          logger.debug(methodName, dcj.getDuccId(), "case: Service");
	          dw = dwHelper.fetch(dcj.getDuccId());
	          IDuccWorkJob service = (IDuccWorkJob) dw;
	          processCmdLine = service.getCommandLine();
	          processCmdLine.addOption("-Dducc.deploy.components=service");
	          break;
	        default:
	          logger.debug(methodName, dcj.getDuccId(), "case: default");
	          dw = dwHelper.fetch(dcj.getDuccId());
	          if(dw instanceof IDuccWorkExecutable) {
	        	  IDuccWorkExecutable dwe = (IDuccWorkExecutable) dw;
	        	  processCmdLine = dwe.getCommandLine();
	          }
		      break;
	        }
	        
	        String dText = "n/a";
	        if(driverCmdLine != null) {
	        	dText = getCmdLine(driverCmdLine);
	        }
	        logger.trace(methodName, dcj.getDuccId(), "driver: "+dText);
	        
	        String pText = "n/a";
	        if(processCmdLine != null) {
	        	pText = getCmdLine(processCmdLine);
	        }
	        logger.trace(methodName, dcj.getDuccId(), "process: "+pText);
	        
	        jobDeploymentList.add( new DuccJobDeployment(dcj.getDuccId(), driverCmdLine,
	                           processCmdLine, 
	                           dcj.getStandardInfo(),
	                           driverProcess,
	                           pma,
	                           //processAdjustedMemorySize,
	                           jobProcessList ));
	      } else if (entry.getValue() instanceof DuccWorkReservation ) {
	        String userId = ((DuccWorkReservation) entry.getValue()).getStandardInfo().getUser();
	        if ( !"System".equals(userId)) {
	          IDuccReservationMap reservationMap = 
	                  ((DuccWorkReservation) entry.getValue()).getReservationMap();
	          reservationList.add(new DuccUserReservation(userId, ((DuccWorkReservation) entry.getValue()).getDuccId(), reservationMap));
	          logger.debug(methodName,null,"---------------  Added reservation for user:"+userId);
	        }
        }
	  }
      logger.info(methodName, null , "---- PM Dispatching DuccJobsStateEvent request to Agent(s) - State Map Size:"+jobDeploymentList.size()+" Reservation List:"+reservationList.size());
      DuccJobsStateEvent ev =  new DuccJobsStateEvent(DuccEvent.EventType.PM_STATE, jobDeploymentList, reservationList);
      ev.setSequence(sequence);
      //  Dispatch state update to agents
      eventDispatcher.dispatch(ev);
      logger.debug(methodName, null , "+++++ PM Dispatched State To Agent(s)");
	  } catch( Throwable t ) {
      logger.error(methodName,null,t);
	  }
	}
	
	private String formatProcess( IDuccProcess process ) {
		return String.format(jobHeaderFormat,
			    String.valueOf(process.getDuccId().getFriendly()),
				process.getProcessType().toString(),
				(process.getPID()==null? "" :process.getPID()),
				process.getProcessState().toString(),
				process.getResourceState().toString(),
				process.getNodeIdentity().getIp(),
				process.getNodeIdentity().getName(),
				process.getProcessDeallocationType().toString(),
				(process.getProcessJmxUrl() == null ? "N/A" : process.getProcessJmxUrl() ));
		
	}
	private void dumpState(Map<DuccId, IDuccWork> workMap) {
		String methodName="dumpState";
		try {
			StringBuffer sb = new StringBuffer();
			for( Entry<DuccId,IDuccWork> job : workMap.entrySet()) {
				IDuccWork duccWork = job.getValue();
				if ( duccWork instanceof DuccWorkJob ) {
					DuccWorkJob duccWorkJob = (DuccWorkJob)duccWork;
					sb.append("\n").append(tbl).
					   append("\nJob ID: ").append(duccWorkJob.getDuccId().getFriendly()).
					   append("\tJobState: ").append(duccWorkJob.getStateObject()).
					   append("\tJobSubmittedBy: ").append(duccWorkJob.getStandardInfo().getUser()).
					   append("\n\n").
					   append(header).append(tbl).append("\n");
					DuccWorkPopDriver driver = duccWorkJob.getDriver();
					if(driver != null) {
						IDuccProcess driverProcess =
							driver.getProcessMap().entrySet().iterator().next().getValue();
							sb.append(formatProcess(driverProcess));
					}
					for(Entry<DuccId,IDuccProcess> process : ((DuccWorkJob)job.getValue()).getProcessMap().entrySet()) {
						sb.append("\n").append(formatProcess(process.getValue()));
					}
					sb.append("\n").append(tbl).append("\n");
					logger.info(methodName, null, sb.toString());
				} else if ( job.getValue() instanceof DuccWorkReservation ) {
					continue;  // TBI
				} else if ( job.getValue() instanceof DuccWorkPop ) {
					continue;  // TBI
				} else {
					logger.info(methodName, job.getKey(), "Not a WorkJob but "+job.getClass().getName());
				}
			}
			
		} catch( Exception e) {
			e.printStackTrace();
		}
	} 
	/**
	 * Override
	 */
	public void setLogLevel(String clz, String level) {
		logger.info("setLogLevel",null,"--------- Changing Log Level to:"+level+ " For class:"+clz);
		super.setLogLevel(clz, level);
	}
	public void setLogLevel(String level) {
		logger.info("setLogLevel",null,"--------- Changing Log Level to:"+level+ " For class:"+getClass().getCanonicalName());
		super.setLogLevel(getClass().getCanonicalName(), level);
	}
	public String getLogLevel() {
		return super.getLogLevel();
	}
	public void logAtTraceLevel(String toLog, String methodName) {
		if ( logger.isTrace()) {
			logger.trace(methodName,null,"--------- "+ toLog);
		}
	}
	

	public PmStateDuccEvent getState() {
		String methodName = "PmStateDuccEvent";
		logger.trace(methodName,null,"");
		return new PmStateDuccEvent();
	}

}
