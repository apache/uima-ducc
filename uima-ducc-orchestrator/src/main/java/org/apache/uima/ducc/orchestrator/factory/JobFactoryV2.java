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
package org.apache.uima.ducc.orchestrator.factory;

import java.io.File;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;

import org.apache.uima.ducc.common.IDuccEnv;
import org.apache.uima.ducc.common.NodeIdentity;
import org.apache.uima.ducc.common.config.CommonConfiguration;
import org.apache.uima.ducc.common.container.FlagsHelper;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.QuotedOptions;
import org.apache.uima.ducc.common.utils.TimeStamp;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.common.utils.id.DuccId;
import org.apache.uima.ducc.common.utils.id.DuccIdFactory;
import org.apache.uima.ducc.common.utils.id.IDuccIdFactory;
import org.apache.uima.ducc.orchestrator.CGroupManager;
import org.apache.uima.ducc.orchestrator.JobDriverHostManager;
import org.apache.uima.ducc.orchestrator.OrchestratorCommonArea;
import org.apache.uima.ducc.transport.cmdline.ACommandLine;
import org.apache.uima.ducc.transport.cmdline.JavaCommandLine;
import org.apache.uima.ducc.transport.cmdline.NonJavaCommandLine;
import org.apache.uima.ducc.transport.event.cli.JobRequestProperties;
import org.apache.uima.ducc.transport.event.cli.JobSpecificationProperties;
import org.apache.uima.ducc.transport.event.cli.ReservationSpecificationProperties;
import org.apache.uima.ducc.transport.event.cli.ServiceRequestProperties;
import org.apache.uima.ducc.transport.event.common.DuccProcess;
import org.apache.uima.ducc.transport.event.common.DuccSchedulingInfo;
import org.apache.uima.ducc.transport.event.common.DuccStandardInfo;
import org.apache.uima.ducc.transport.event.common.DuccUimaAggregate;
import org.apache.uima.ducc.transport.event.common.DuccUimaAggregateComponent;
import org.apache.uima.ducc.transport.event.common.DuccUimaDeploymentDescriptor;
import org.apache.uima.ducc.transport.event.common.DuccWorkJob;
import org.apache.uima.ducc.transport.event.common.DuccWorkPopDriver;
import org.apache.uima.ducc.transport.event.common.IDuccCommand;
import org.apache.uima.ducc.transport.event.common.IDuccProcessType.ProcessType;
import org.apache.uima.ducc.transport.event.common.IDuccTypes.DuccType;
import org.apache.uima.ducc.transport.event.common.IDuccUimaAggregate;
import org.apache.uima.ducc.transport.event.common.IDuccUimaAggregateComponent;
import org.apache.uima.ducc.transport.event.common.IDuccUimaDeploymentDescriptor;
import org.apache.uima.ducc.transport.event.common.IDuccUnits.MemoryUnits;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService.ServiceDeploymentType;
import org.apache.uima.ducc.transport.event.common.IResourceState.ResourceState;

public class JobFactoryV2 implements IJobFactory {
	private static JobFactoryV2 jobFactory = new JobFactoryV2();
	private static final DuccLogger logger = DuccLoggerComponents.getOrLogger(JobFactoryV2.class.getName());
	
	public static IJobFactory getInstance() {
		return jobFactory;
	}
	
	private long driver_max_size_in_bytes = 0;

	private JobFactoryV2() {
		String ducc_jd_share_quantum = DuccPropertiesResolver.getInstance().getFileProperty(DuccPropertiesResolver.ducc_jd_share_quantum);
		long oneKB = 1024;
		long oneMB = 1024*oneKB;
		driver_max_size_in_bytes = Long.parseLong(ducc_jd_share_quantum) * oneMB;
	}
	
	private OrchestratorCommonArea orchestratorCommonArea = OrchestratorCommonArea.getInstance();
	private IDuccIdFactory duccIdFactory = orchestratorCommonArea.getDuccIdFactory();
	private JobDriverHostManager hostManager = orchestratorCommonArea.getHostManager();
	private DuccIdFactory jdIdFactory = new DuccIdFactory();
	
	private String classpath_order = System.getProperty("ducc.orchestrator.job.factory.classpath.order");
	
	private int addEnvironment(DuccWorkJob job, String type, ACommandLine aCommandLine, String environmentVariables) {
		String methodName = "addEnvironment";
		logger.trace(methodName, job.getDuccId(), "enter");
		int retVal = 0;
		if(environmentVariables != null) {
			logger.debug(methodName, job.getDuccId(), environmentVariables);
			// Tokenize the list of assignments, dequote, and convert to a map of environment settings
			ArrayList<String> envVarList = QuotedOptions.tokenizeList(environmentVariables, true);
			Map<String, String> envMap;
			try {
			    envMap = QuotedOptions.parseAssignments(envVarList, false);
			} catch (IllegalArgumentException e) {
                logger.warn(methodName, job.getDuccId(),"Invalid environment syntax in: " + environmentVariables);
                return 0;  // Should not happen as CLI should have checked and rejected the request
			}
			aCommandLine.addEnvironment(envMap);
			retVal = envMap.size();
		}
		logger.trace(methodName, job.getDuccId(), "exit");
		return retVal;
	}
	
	private void checkSpec(DuccWorkJob job, JobRequestProperties jobRequestProperties) {
		String methodName = "checkSpec";
		logger.trace(methodName, job.getDuccId(), "enter");
		jobRequestProperties.normalize();
		Enumeration<Object> keys = jobRequestProperties.keys();
		while(keys.hasMoreElements()) {
			String key = (String) keys.nextElement();
			if(!jobRequestProperties.isRecognized(key)) {
				logger.warn(methodName, job.getDuccId(), "unrecognized: "+key);
			}
		}
		logger.trace(methodName, job.getDuccId(), "exit");
		return;
	}
	
	private ArrayList<String> toArrayList(String overrides) {
		String methodName = "toArrayList";
		logger.trace(methodName, null, "enter");
		// To match other lists tokenize on blanks & strip any quotes around values.
		ArrayList<String> list = QuotedOptions.tokenizeList(overrides, true);
		logger.trace(methodName, null, "exit");
		return list;
	}

	private void dump(DuccWorkJob job, IDuccUimaAggregate uimaAggregate) {
		String methodName = "dump";
		logger.info(methodName, job.getDuccId(), "brokerURL     "+uimaAggregate.getBrokerURL());
		logger.info(methodName, job.getDuccId(), "endpoint      "+uimaAggregate.getEndpoint());
		logger.info(methodName, job.getDuccId(), "description   "+uimaAggregate.getDescription());
		logger.info(methodName, job.getDuccId(), "name          "+uimaAggregate.getName());
		logger.info(methodName, job.getDuccId(), "thread-count  "+uimaAggregate.getThreadCount());
		List<IDuccUimaAggregateComponent> components = uimaAggregate.getComponents();
		for(IDuccUimaAggregateComponent component : components) {
			logger.info(methodName, job.getDuccId(), "descriptor    "+component.getDescriptor());
			List<String> overrides = component.getOverrides();
			for(String override : overrides) {
				logger.info(methodName, job.getDuccId(), "override      "+override);
			}
		}
	}
	
	private void dump(DuccWorkJob job, IDuccUimaDeploymentDescriptor uimaDeploymentDescriptor) {
		String methodName = "dump";
		logger.info(methodName, job.getDuccId(), "uimaDeploymentDescriptor      "+uimaDeploymentDescriptor);
	}
	
	private void logSweeper(String logDir, DuccId jobId) {
		String methodName = "logSweeper";
		if(logDir != null) {
			if(jobId != null) {
				if(!logDir.endsWith(File.separator)) {
					logDir += File.separator;
				}
				logDir += jobId;
				try {
					File file = new File(logDir);
					if(file.exists()) {
						File dest = new File(logDir+"."+"sweep"+"."+java.util.Calendar.getInstance().getTime().toString());
						file.renameTo(dest);
						logger.warn(methodName, jobId, "renamed "+logDir);
					}
				}
				catch(Throwable t) {
					logger.warn(methodName, jobId, "unable to rename "+logDir, t);
				}
			}
			else {
				logger.warn(methodName, jobId, "jobId is null");
			}
		}
		else {
			logger.warn(methodName, jobId, "logDir is null");
		}
	}
	
	private boolean isClasspathOrderUserBeforeDucc(String user_specified_classpath_order, DuccId jobId) {
		String methodName = "isClasspathOrderUserBeforeDucc";
		boolean retVal = false;
		if(user_specified_classpath_order != null) {
			if(user_specified_classpath_order.trim().equals("user-before-ducc")) {
				logger.warn(methodName, jobId, "user specified classpath order: "+user_specified_classpath_order);
				retVal = true;
			}
		}
		else if(classpath_order != null) {
			if(classpath_order.trim().equals("user-before-ducc")) {
				retVal = true;
			}
		}
		return retVal;
	}
	
	private boolean isJpUima(DuccType duccType, ServiceDeploymentType serviceDeploymentType) {
		boolean retVal = true;
		switch(duccType) {
		case Job:
			break;
		case Service:
			switch(serviceDeploymentType) {
			case uima:
				break;
			case custom:	
			case other:	
			default:
				retVal = false;
				break;
			}
			break;
		case Reservation:
		case Pop:
		case Undefined:
		default:
			//huh?
			retVal = false;
			break;
		}
		return retVal;
	}
	
	private void setDebugPorts(CommonConfiguration common, JobRequestProperties jobRequestProperties,  DuccWorkJob job) {
		String location = "setDebugPorts";
		DuccId jobid = job.getDuccId();
		String portDriver = jobRequestProperties.getProperty(JobSpecificationProperties.key_driver_debug);
		if(portDriver != null) {
			try {
				long port = Long.parseLong(portDriver);
				job.setDebugPortDriver(port);
				logger.debug(location, jobid, "Driver debug port: "+job.getDebugPortDriver());
			}
			catch(Exception e) {
				logger.error(location, jobid, "Invalid driver debug port: "+portDriver);
			}
		}
		String portProcess = jobRequestProperties.getProperty(JobSpecificationProperties.key_process_debug);
		if(portProcess != null) {
			try {
				long port = Long.parseLong(portProcess);
				job.setDebugPortProcess(port);
				logger.debug(location, jobid, "Process debug port: "+job.getDebugPortProcess());
			}
			catch(Exception e) {
				logger.error(location, jobid, "Invalid process debug port: "+portProcess);
			}
		}
	}
	
	private String buildJobDriverClasspath(JobRequestProperties jobRequestProperties, DuccId jobid) {
		String methodName = "buildJobDriverClasspath";
		String cp = null;
		cp = getDuccClasspath(0);
		logger.debug(methodName, jobid, "java CP:"+cp);	
		return cp;
	}
	
	private JavaCommandLine buildJobDriverCommandLine(JobRequestProperties jobRequestProperties, DuccId jobid) {
		JavaCommandLine jcl = null;
		// java command
		String javaCmd = jobRequestProperties.getProperty(JobSpecificationProperties.key_jvm);
		jcl = new JavaCommandLine(javaCmd);
		jcl.setClassName(IDuccCommand.main);
		jcl.addOption(IDuccCommand.arg_ducc_deploy_configruation);
		jcl.addOption(IDuccCommand.arg_ducc_deploy_components);
		jcl.addOption(IDuccCommand.arg_ducc_job_id+jobid.toString());
		String cp = buildJobDriverClasspath(jobRequestProperties, jobid);
		jcl.setClasspath(cp);
		// Add the user-provided JVM args
		boolean haveXmx = false;
		String driver_jvm_args = jobRequestProperties.getProperty(JobSpecificationProperties.key_driver_jvm_args);
		ArrayList<String> dTokens = QuotedOptions.tokenizeList(driver_jvm_args, true);
		for(String token : dTokens) {
			jcl.addOption(token);
			if (!haveXmx) {
			    haveXmx = token.startsWith("-Xmx");
			}
		}
		// Add any site-provided JVM args, but not -Xmx if the user has provided one
		String siteJvmArgs = DuccPropertiesResolver.getInstance().getFileProperty(DuccPropertiesResolver.ducc_driver_jvm_args);
		dTokens = QuotedOptions.tokenizeList(siteJvmArgs, true);    // a null arg is acceptable
		for (String token : dTokens) {
		    if (!haveXmx || !token.startsWith("-Xmx")) {
		       jcl.addOption(token);
		    }
		}
		// Add job JVM args
			// add JobId
			String opt;
			opt = FlagsHelper.Name.JobId.dname()+"="+jobid.getFriendly();
			jcl.addOption(opt);
			// add CrXML
			String crxml = jobRequestProperties.getProperty(JobSpecificationProperties.key_driver_descriptor_CR);
			if(crxml != null) {
				opt = FlagsHelper.Name.CollectionReaderXml.dname()+"="+crxml;
				jcl.addOption(opt);
			}
			// add CrCfg
			String crcfg = jobRequestProperties.getProperty(JobSpecificationProperties.key_driver_descriptor_CR_overrides);
			if(crcfg != null) {
				opt = FlagsHelper.Name.CollectionReaderCfg.dname()+"="+crcfg;
				jcl.addOption(opt);
			}
			// add userCP
			String userCP = jobRequestProperties.getProperty(JobSpecificationProperties.key_classpath);
			if(userCP == null) {
				userCP = "";
			}
			String augment = IDuccEnv.DUCC_HOME+File.separator+"lib"+File.separator+"uima-ducc"+File.separator+"*";
			userCP = augment+File.pathSeparator+userCP;
			opt = FlagsHelper.Name.UserClasspath.dname()+"="+userCP;
			jcl.addOption(opt);
		// Name the log config file explicitly - the default of searching the user-provided classpath is dangerous
		jcl.addOption("-Dlog4j.configuration=file://" + Utils.findDuccHome() + "/resources/log4j.xml");
		// Log directory
		jcl.setLogDirectory(jobRequestProperties.getProperty(JobSpecificationProperties.key_log_directory));
		return jcl;
	}
	
	private void createDriver(CommonConfiguration common, JobRequestProperties jobRequestProperties,  DuccWorkJob job) {
		String methodName = "createDriver";
		DuccPropertiesResolver duccPropertiesResolver = DuccPropertiesResolver.getInstance();
		// broker & queue
		job.setJobBroker(common.brokerUrl);
		job.setJobQueue(common.jdQueuePrefix+job.getDuccId());
		// CR
		String crxml = jobRequestProperties.getProperty(JobSpecificationProperties.key_driver_descriptor_CR);
		String crcfg = jobRequestProperties.getProperty(JobSpecificationProperties.key_driver_descriptor_CR_overrides);
		// getMeta
		String meta_time = duccPropertiesResolver.getFileProperty(DuccPropertiesResolver.default_process_get_meta_time_max);
		// lost
		String lost_time = duccPropertiesResolver.getFileProperty(DuccPropertiesResolver.ducc_jd_queue_timeout_minutes);
		// process_per_item_time_max
		String wi_time = jobRequestProperties.getProperty(JobRequestProperties.key_process_per_item_time_max);
		if(wi_time == null) {
			wi_time = duccPropertiesResolver.getFileProperty(DuccPropertiesResolver.default_process_per_item_time_max);
		}
		// Exception handler
		String processExceptionHandler = jobRequestProperties.getProperty(JobRequestProperties.key_driver_exception_handler);
		// ProcessStatusMaxWaitMillis
		long processStatusMaxWaitMillis = -1;
		try {
			String ducc_orchestrator_abbreviated_state_publish_rate = duccPropertiesResolver.getFileProperty(DuccPropertiesResolver.ducc_orchestrator_abbreviated_state_publish_rate);
			String ducc_rm_state_publish_rate = duccPropertiesResolver.getFileProperty(DuccPropertiesResolver.ducc_rm_state_publish_rate);
			String ducc_agent_node_metrics_publish_rate = duccPropertiesResolver.getFileProperty(DuccPropertiesResolver.ducc_agent_node_metrics_publish_rate);
			String ducc_rm_node_stability = duccPropertiesResolver.getFileProperty(DuccPropertiesResolver.ducc_rm_node_stability);
			long orMillisRate = Long.parseLong(ducc_orchestrator_abbreviated_state_publish_rate);
			long rmMillisRate = Long.parseLong(ducc_rm_state_publish_rate);
			long metricsMillisRate = Long.parseLong(ducc_agent_node_metrics_publish_rate);
			long stabilityCount = Long.parseLong(ducc_rm_node_stability);
			long fudgeMillis = 10 * 1000;
			processStatusMaxWaitMillis = metricsMillisRate*stabilityCount+(orMillisRate+rmMillisRate+fudgeMillis);
			logger.debug(methodName, job.getDuccId(), "driver:processStatusMaxWaitMillis="+processStatusMaxWaitMillis);
		}
		catch(Exception e) {
			logger.error(methodName, job.getDuccId(), e);
		}
		// Command line
		DuccWorkPopDriver driver = new DuccWorkPopDriver(job.getjobBroker(), job.getjobQueue(), crxml, crcfg, meta_time, lost_time, wi_time, processExceptionHandler, processStatusMaxWaitMillis);
		JavaCommandLine driverCommandLine = buildJobDriverCommandLine(jobRequestProperties, job.getDuccId());
		// Environment
		String driverEnvironmentVariables = jobRequestProperties.getProperty(JobSpecificationProperties.key_environment);
		int envCountDriver = addEnvironment(job, "driver", driverCommandLine, driverEnvironmentVariables);
		logger.info(methodName, job.getDuccId(), "driver env vars: "+envCountDriver);
		logger.debug(methodName, job.getDuccId(), "driver: "+driverCommandLine.getCommand());
		driver.setCommandLine(driverCommandLine);
		//
		NodeIdentity nodeIdentity = hostManager.getNode();
		DuccId duccId = jdIdFactory.next();
		duccId.setFriendly(0);
		DuccProcess driverProcess = new DuccProcess(duccId,nodeIdentity,ProcessType.Pop);
		CGroupManager.assign(job.getDuccId(), driverProcess, driver_max_size_in_bytes);
		driverProcess.setResourceState(ResourceState.Allocated);
		driverProcess.setNodeIdentity(nodeIdentity);
		driver.getProcessMap().put(driverProcess.getDuccId(), driverProcess);
		//
		orchestratorCommonArea.getProcessAccounting().addProcess(duccId, job.getDuccId());
		//
		job.setDriver(driver);
	}
	
	private void checkSchedulingLimits(DuccWorkJob job, DuccSchedulingInfo schedulingInfo) {
	    String methodName = "checkSpec";
        long limit = 0;
        String p_limit = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_threads_limit);
        if (p_limit != null) {
            if (!p_limit.equals("unlimited")) {
                limit = Long.parseLong(p_limit);
            }
        }
        if (limit <= 0) {
            return;
        }
        int threads_per_share = schedulingInfo.getIntThreadsPerShare();
        long sharesLimit = limit / threads_per_share;
        long maxShares = schedulingInfo.getLongSharesMax();
        if (maxShares == 0 || maxShares > sharesLimit) {
            logger.info(methodName, job.getDuccId(), "change max-shares from "+maxShares+" to " + sharesLimit);
            schedulingInfo.setSharesMax(String.valueOf(sharesLimit));
        }
	}
	
	public DuccWorkJob create(CommonConfiguration common, JobRequestProperties jobRequestProperties) {
		String methodName = "create";
		DuccWorkJob job = new DuccWorkJob();
		checkSpec(job, jobRequestProperties);
		// id, type
		String ddCr = jobRequestProperties.getProperty(JobSpecificationProperties.key_driver_descriptor_CR);
		if(ddCr == null) {
			job.setDuccType(DuccType.Service);
			job.setDuccId(duccIdFactory.next());
		}
		else {
			job.setDuccType(DuccType.Job);
			job.setDuccId(duccIdFactory.next());
			
		}
		// driver
		DuccType duccType = job.getDuccType();
		switch(duccType) {
			case Job:
				createDriver(common, jobRequestProperties, job);
				setDebugPorts(common, jobRequestProperties, job);
				break;
			case Service:
				break;
		}
        // Service Deployment Type
        if(jobRequestProperties.containsKey(ServiceRequestProperties.key_service_type_custom)) {
			job.setServiceDeploymentType(ServiceDeploymentType.custom);
		}
        else if(jobRequestProperties.containsKey(ServiceRequestProperties.key_service_type_other)) {
			job.setServiceDeploymentType(ServiceDeploymentType.other);
		}
        else if(jobRequestProperties.containsKey(ServiceRequestProperties.key_service_type_uima)) {
			job.setServiceDeploymentType(ServiceDeploymentType.uima);
		}
        else {
        	job.setServiceDeploymentType(ServiceDeploymentType.unspecified);
        }
        // Service Id
        String serviceId = null;
        if(jobRequestProperties.containsKey(ServiceRequestProperties.key_service_id)) {
        	serviceId = jobRequestProperties.getProperty(ServiceRequestProperties.key_service_id);
        }
        job.setServiceId(serviceId);
		// sweep out leftover logging trash
		logSweeper(jobRequestProperties.getProperty(JobRequestProperties.key_log_directory), job.getDuccId());
		// log
		jobRequestProperties.specification(logger);
		// classpath
        String java_classpath = getDuccClasspath(1);    // for job processes & services 
		String processClasspath = jobRequestProperties.getProperty(JobSpecificationProperties.key_classpath);
		logger.debug(methodName, job.getDuccId(), "process CP (spec):"+processClasspath);
		logger.debug(methodName, job.getDuccId(), "java CP:"+java_classpath);
		if(processClasspath != null) {
			if(isClasspathOrderUserBeforeDucc(jobRequestProperties.getProperty(JobSpecificationProperties.key_classpath_order),job.getDuccId())) {
				logger.info(methodName, job.getDuccId(), "process:OrderUserBeforeDucc");
				processClasspath=processClasspath+File.pathSeparator+java_classpath;
			}
			else {
				logger.info(methodName, job.getDuccId(), "process:OrderDuccBeforeUser");
				processClasspath=java_classpath+File.pathSeparator+processClasspath;
			}
		}
		else {
			logger.info(methodName, job.getDuccId(), "process:OrderDefault");
			processClasspath=java_classpath;
		}
		logger.debug(methodName, job.getDuccId(), "process CP (combined):"+processClasspath);
		// java command
		String javaCmd = jobRequestProperties.getProperty(JobSpecificationProperties.key_jvm);
		if(javaCmd == null) {
            // Agent will set javaCmd for Driver and Processes
		}
		// standard info
		DuccStandardInfo standardInfo = new DuccStandardInfo();
		job.setStandardInfo(standardInfo);
		standardInfo.setUser(jobRequestProperties.getProperty(JobSpecificationProperties.key_user));
		standardInfo.setSubmitter(jobRequestProperties.getProperty(JobSpecificationProperties.key_submitter_pid_at_host));
		standardInfo.setDateOfSubmission(TimeStamp.getCurrentMillis());
		standardInfo.setDateOfCompletion(null);
		standardInfo.setDescription(jobRequestProperties.getProperty(JobSpecificationProperties.key_description));
		standardInfo.setLogDirectory(jobRequestProperties.getProperty(JobSpecificationProperties.key_log_directory));
		standardInfo.setWorkingDirectory(jobRequestProperties.getProperty(JobSpecificationProperties.key_working_directory));
		String notifications = jobRequestProperties.getProperty(JobSpecificationProperties.key_notifications);
		if(notifications == null) {
			standardInfo.setNotifications(null);
		}
		else {
			String[] notificationsArray = notifications.split(" ,");
			for(String notification : notificationsArray) {
				notification.trim();
			}
			standardInfo.setNotifications(notificationsArray);
		}
		// scheduling info
		DuccSchedulingInfo schedulingInfo = new DuccSchedulingInfo();
		String ducc_rm_share_quantum = DuccPropertiesResolver.getInstance().getFileProperty(DuccPropertiesResolver.ducc_rm_share_quantum);
		if(ducc_rm_share_quantum != null) {
			ducc_rm_share_quantum = ducc_rm_share_quantum.trim();
			if(ducc_rm_share_quantum.length() > 0) {
				schedulingInfo.setShareMemorySize(ducc_rm_share_quantum);
			}
		}
		job.setSchedulingInfo(schedulingInfo);
		schedulingInfo.setSchedulingClass(jobRequestProperties.getProperty(JobSpecificationProperties.key_scheduling_class));
		schedulingInfo.setSchedulingPriority(jobRequestProperties.getProperty(JobSpecificationProperties.key_scheduling_priority));
		schedulingInfo.setSharesMax(jobRequestProperties.getProperty(JobSpecificationProperties.key_process_deployments_max));
		schedulingInfo.setSharesMin(jobRequestProperties.getProperty(JobSpecificationProperties.key_process_deployments_min));
		schedulingInfo.setThreadsPerShare(jobRequestProperties.getProperty(JobSpecificationProperties.key_process_thread_count));
		schedulingInfo.setShareMemorySize(jobRequestProperties.getProperty(JobSpecificationProperties.key_process_memory_size));
		schedulingInfo.setShareMemoryUnits(MemoryUnits.GB);
		
		if (job.getDuccType() == DuccType.Job){ 
		    checkSchedulingLimits(job, schedulingInfo);
		}
		
		// process_initialization_time_max
		String pi_time = jobRequestProperties.getProperty(JobRequestProperties.key_process_initialization_time_max);
		if(pi_time == null) {
			pi_time = DuccPropertiesResolver.getInstance().getFileProperty(DuccPropertiesResolver.ducc_agent_launcher_process_init_timeout);
		}
		try {
			long value = Long.parseLong(pi_time)*60*1000;
			standardInfo.setProcessInitializationTimeMax(value);
		}
		catch(Exception e) {
			logger.error(methodName, job.getDuccId(), e);
		}
		// jp
		ServiceDeploymentType serviceDeploymentType = job.getServiceDeploymentType();
		if(isJpUima(duccType, serviceDeploymentType)) {
			String process_DD = jobRequestProperties.getProperty(JobSpecificationProperties.key_process_DD);
			if(process_DD != null) {
				// user DD
				IDuccUimaDeploymentDescriptor uimaDeploymentDescriptor = new DuccUimaDeploymentDescriptor(process_DD);
				job.setUimaDeployableConfiguration(uimaDeploymentDescriptor);
				dump(job, uimaDeploymentDescriptor);
			}
			else {
				// UIMA aggregate
				String name = common.jdQueuePrefix+job.getDuccId().toString();
				String description = job.getStandardInfo().getDescription();
				int threadCount = Integer.parseInt(job.getSchedulingInfo().getThreadsPerShare());
				String brokerURL = job.getjobBroker();;
				String endpoint = job.getjobQueue();
				ArrayList<IDuccUimaAggregateComponent> components = new ArrayList<IDuccUimaAggregateComponent>();
				String CMDescriptor = jobRequestProperties.getProperty(JobSpecificationProperties.key_process_descriptor_CM);
				if(CMDescriptor != null) {
					ArrayList<String> CMOverrides = toArrayList(jobRequestProperties.getProperty(JobSpecificationProperties.key_process_descriptor_CM_overrides));
					IDuccUimaAggregateComponent componentCM = new DuccUimaAggregateComponent(CMDescriptor, CMOverrides);
					components.add(componentCM);
				}
				String AEDescriptor = jobRequestProperties.getProperty(JobSpecificationProperties.key_process_descriptor_AE);
				if(AEDescriptor != null) {
					ArrayList<String> AEOverrides = toArrayList(jobRequestProperties.getProperty(JobSpecificationProperties.key_process_descriptor_AE_overrides));
					IDuccUimaAggregateComponent componentAE = new DuccUimaAggregateComponent(AEDescriptor, AEOverrides);
					components.add(componentAE);
				}
				String CCDescriptor = jobRequestProperties.getProperty(JobSpecificationProperties.key_process_descriptor_CC);
				if(CCDescriptor != null) {
					ArrayList<String> CCOverrides = toArrayList(jobRequestProperties.getProperty(JobSpecificationProperties.key_process_descriptor_CC_overrides));
					IDuccUimaAggregateComponent componentCC = new DuccUimaAggregateComponent(CCDescriptor, CCOverrides);
					components.add(componentCC);
				}
				IDuccUimaAggregate uimaAggregate = new DuccUimaAggregate(name,description,threadCount,brokerURL,endpoint,components);
				job.setUimaDeployableConfiguration(uimaAggregate);
				dump(job, uimaAggregate);
			}
			// pipelines
			JavaCommandLine pipelineCommandLine = new JavaCommandLine(javaCmd);
			pipelineCommandLine.setClassName("main:provided-by-Process-Manager");
			pipelineCommandLine.setClasspath(processClasspath);
			String process_jvm_args = jobRequestProperties.getProperty(JobSpecificationProperties.key_process_jvm_args);
			ArrayList<String> pTokens = QuotedOptions.tokenizeList(process_jvm_args, true);
			for(String token : pTokens) {
				pipelineCommandLine.addOption(token);
			}
		    // Add any site-provided JVM args
	        String siteJvmArgs = DuccPropertiesResolver.getInstance().getFileProperty(DuccPropertiesResolver.ducc_process_jvm_args);
	        pTokens = QuotedOptions.tokenizeList(siteJvmArgs, true);   // a null arg is acceptable
	        for(String token : pTokens) {
	            pipelineCommandLine.addOption(token);
	        }
	        
			// add userCP
			String userCP = jobRequestProperties.getProperty(JobSpecificationProperties.key_classpath);
			if(userCP == null) {
				userCP = "";
			}
			String augment = IDuccEnv.DUCC_HOME+File.separator+"lib"+File.separator+"uima-ducc"+File.separator+"*";
			userCP = augment+File.pathSeparator+userCP;
			String opt = FlagsHelper.Name.UserClasspath.dname()+"="+userCP;
			pipelineCommandLine.addOption(opt);
			
			String processEnvironmentVariables = jobRequestProperties.getProperty(JobSpecificationProperties.key_environment);
			int envCountProcess = addEnvironment(job, "process", pipelineCommandLine, processEnvironmentVariables);
			logger.info(methodName, job.getDuccId(), "process env vars: "+envCountProcess);
			logger.debug(methodName, job.getDuccId(), "pipeline: "+pipelineCommandLine.getCommand());
			pipelineCommandLine.setLogDirectory(jobRequestProperties.getProperty(JobSpecificationProperties.key_log_directory));
			job.setCommandLine(pipelineCommandLine);
		}
		else {
			// ducclet (sometimes known as arbitrary process)
			String process_executable = jobRequestProperties.getProperty(JobSpecificationProperties.key_process_executable);
			NonJavaCommandLine executableProcessCommandLine = new NonJavaCommandLine(process_executable);
			String processEnvironmentVariables = jobRequestProperties.getProperty(JobSpecificationProperties.key_environment);
			int envCountProcess = addEnvironment(job, "process", executableProcessCommandLine, processEnvironmentVariables);
			logger.info(methodName, job.getDuccId(), "process env vars: "+envCountProcess);
			logger.debug(methodName, job.getDuccId(), "ducclet: "+executableProcessCommandLine.getCommandLine());
			job.setCommandLine(executableProcessCommandLine);
			// Tokenize arguments string and strip any quotes, then add to command line.
			// Note: placeholders replaced by CLI so can avoid the add method.
			List<String> process_executable_arguments = QuotedOptions.tokenizeList(
			        jobRequestProperties.getProperty(JobSpecificationProperties.key_process_executable_args), true);
			executableProcessCommandLine.getArguments().addAll(process_executable_arguments);
		}
		// process_initialization_failures_cap
		String failures_cap = jobRequestProperties.getProperty(JobSpecificationProperties.key_process_initialization_failures_cap);
		try {
			long process_failures_cap = Long.parseLong(failures_cap);
			if(process_failures_cap > 0) {
				job.setProcessInitFailureCap(process_failures_cap);
			}
			else {
				logger.warn(methodName, job.getDuccId(), "invalid "+JobSpecificationProperties.key_process_initialization_failures_cap+": "+failures_cap);
			}
		}
		catch(Exception e) {
			logger.warn(methodName, job.getDuccId(), "invalid "+JobSpecificationProperties.key_process_initialization_failures_cap+": "+failures_cap);
		}
		// process_failures_limit
		String failures_limit = jobRequestProperties.getProperty(JobSpecificationProperties.key_process_failures_limit);
		try {
			long process_failures_limit = Long.parseLong(failures_limit);
			if(process_failures_limit > 0) {
				job.setProcessFailureLimit(process_failures_limit);
			}
			else {
				logger.warn(methodName, job.getDuccId(), "invalid "+JobSpecificationProperties.key_process_failures_limit+": "+failures_limit);
			}
		}
		catch(Exception e) {
			logger.warn(methodName, job.getDuccId(), "invalid "+JobSpecificationProperties.key_process_failures_limit+": "+failures_limit);
		}
        //
        // Set the service dependency, if there is one.
        //
        String depstr = jobRequestProperties.getProperty(JobSpecificationProperties.key_service_dependency);
        if ( depstr == null ) {            
            logger.debug(methodName, job.getDuccId(), "No service dependencies");
        } else {
            logger.debug(methodName, job.getDuccId(), "Adding service dependency", depstr);
            String[] deps = depstr.split("\\s+");      
            job.setServiceDependencies(deps);
        }
        // Service Endpoint
        String ep = jobRequestProperties.getProperty(ServiceRequestProperties.key_service_request_endpoint);
        if ( ep == null ) {                     
            logger.debug(methodName, job.getDuccId(), "No service endpoint");
        } else {
            logger.debug(methodName, job.getDuccId(), "Adding service endpoint", ep);
            job.setServiceEndpoint(ep);
        }
        // Cancel On Interrupt
        if(jobRequestProperties.containsKey(JobSpecificationProperties.key_cancel_on_interrupt)) {
        	job.setCancelOnInterrupt();
        }
        else if(jobRequestProperties.containsKey(ReservationSpecificationProperties.key_cancel_managed_reservation_on_interrupt)) {
        	job.setCancelOnInterrupt();
        }
		//TODO be sure to clean-up fpath upon job completion!
		return job;
	}
	
	/*
	 * Get minimal subset of the DUCC classpath for job driver & job processes
	 * Cache the values unless asked to reload when testing
	 */
	private String[] cps = null;
	private String getDuccClasspath(int type) {
	    if (cps != null) {
	        return cps[type];
	    }
	    DuccProperties props = new DuccProperties();
	    try {
	        props.load(IDuccEnv.DUCC_CLASSPATH_FILE);
	    } catch (Exception e) {
	        logger.error("getClasspath", null, "Using full classpath as failed to load " + IDuccEnv.DUCC_CLASSPATH_FILE);
	        return System.getProperty("java.class.path");
	    }
	    // If reload specified don't cache the results (for ease of testing changes to the classpaths)
	    if (props.getProperty("ducc.reload.file") != null) {
	        return props.getProperty(type==0 ? "ducc.jobdriver.classpath" : "ducc.jobprocess.classpath");
	    } else {
	        cps = new String[2];
	        cps[0] = props.getProperty("ducc.jobdriver.classpath");
	        cps[1] = props.getProperty("ducc.jobprocess.classpath");
	        return cps[type];
	    }
	}
}
