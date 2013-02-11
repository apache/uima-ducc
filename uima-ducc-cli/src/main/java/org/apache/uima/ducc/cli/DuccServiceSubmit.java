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
package org.apache.uima.ducc.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.uima.ducc.api.DuccMessage;
import org.apache.uima.ducc.api.IDuccMessageProcessor;
import org.apache.uima.ducc.cli.IServiceApi.RegistrationOption;
import org.apache.uima.ducc.common.IDucc;
import org.apache.uima.ducc.common.crypto.Crypto;
import org.apache.uima.ducc.common.exception.DuccRuntimeException;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.IOHelper;
import org.apache.uima.ducc.transport.dispatcher.DuccEventHttpDispatcher;
import org.apache.uima.ducc.transport.event.DuccEvent;
import org.apache.uima.ducc.transport.event.SubmitServiceDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitServiceReplyDuccEvent;
import org.apache.uima.ducc.transport.event.cli.JobRequestProperties;
import org.apache.uima.ducc.transport.event.cli.ServiceRequestProperties;
import org.apache.uima.ducc.transport.event.cli.ServiceSpecificationProperties;
import org.apache.uima.ducc.transport.event.cli.SpecificationProperties;
import org.apache.uima.ducc.transport.event.common.IDuccWorkService.ServiceDeploymentType;
import org.apache.uima.ducc.transport.event.sm.IService.ServiceType;


/**
 * Submit a DUCC service
 */

public class DuccServiceSubmit extends DuccUi {
	
    private String jvmarg_string = null;
    private Properties jvmargs = null;
	private IDuccMessageProcessor duccMessageProcessor = new DuccMessage();
	
	private ServiceDeploymentType serviceDeploymentType = ServiceDeploymentType.unspecified;
	
	public DuccServiceSubmit() {
	}
	
	public DuccServiceSubmit(IDuccMessageProcessor duccMessageProcessor) {
		this.duccMessageProcessor = duccMessageProcessor;
	}
	
    @SuppressWarnings("static-access")
	private void addOptions(Options options) {
		options.addOption(OptionBuilder
				.withDescription(DuccUiConstants.desc_help).hasArg(false)
				.withLongOpt(DuccUiConstants.name_help).create());
		options.addOption(OptionBuilder
				.withDescription(DuccUiConstants.desc_debug).hasArg(false)
				.withLongOpt(DuccUiConstants.name_debug).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_description)
				.withDescription(makeDesc(DuccUiConstants.desc_description,DuccUiConstants.exmp_description)).hasArg()
				.withLongOpt(DuccUiConstants.name_description).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_scheduling_class)
				.withDescription(makeDesc(DuccUiConstants.desc_scheduling_class,DuccUiConstants.exmp_scheduling_class)).hasArg()
				.withLongOpt(DuccUiConstants.name_scheduling_class).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_log_directory)
				.withDescription(makeDesc(DuccUiConstants.desc_log_directory,DuccUiConstants.exmp_log_directory)).hasArg()
				.withLongOpt(DuccUiConstants.name_log_directory).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_working_directory)
				.withDescription(makeDesc(DuccUiConstants.desc_working_directory,DuccUiConstants.exmp_working_directory)).hasArg()
				.withLongOpt(DuccUiConstants.name_working_directory).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_jvm)
				.withDescription(makeDesc(DuccUiConstants.desc_jvm,DuccUiConstants.exmp_jvm)).hasArg()
				.withLongOpt(DuccUiConstants.name_jvm).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_process_jvm_args)
				.withDescription(makeDesc(DuccUiConstants.desc_process_jvm_args,DuccUiConstants.exmp_process_jvm_args)).hasArg()
				.withLongOpt(DuccUiConstants.name_process_jvm_args).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_process_classpath)
				.withDescription(makeDesc(DuccUiConstants.desc_process_classpath,DuccUiConstants.exmp_process_classpath)).hasArg()
				.withLongOpt(DuccUiConstants.name_process_classpath).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_process_environment)
				.withDescription(makeDesc(DuccUiConstants.desc_process_environment,DuccUiConstants.exmp_process_environment)).hasArg()
				.withLongOpt(DuccUiConstants.name_process_environment).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_process_memory_size)
				.withDescription(makeDesc(DuccUiConstants.desc_process_memory_size,DuccUiConstants.exmp_process_memory_size)).hasArg()
				.withLongOpt(DuccUiConstants.name_process_memory_size).create());
		// <UIMA service only>
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_process_DD)
				.withDescription(makeDesc(DuccUiConstants.desc_process_DD,DuccUiConstants.exmp_process_DD)).hasArg()
				.withLongOpt(DuccUiConstants.name_process_DD).create());
		
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_process_deployments_max)
				.withDescription(makeDesc(DuccUiConstants.desc_process_deployments_max,DuccUiConstants.exmp_process_deployments_max)).hasArg()
				.withLongOpt(DuccUiConstants.name_process_deployments_max).create());
		/*
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_process_deployments_min)
				.withDescription(makeDesc(DuccUiConstants.desc_process_deployments_min,DuccUiConstants.exmp_process_deployments_min)).hasArg()
				.withLongOpt(DuccUiConstants.name_process_deployments_min).create());
		*/
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_process_initialization_failures_cap)
				.withDescription(makeDesc(DuccUiConstants.desc_process_initialization_failures_cap,DuccUiConstants.exmp_process_initialization_failures_cap)).hasArg()
				.withLongOpt(DuccUiConstants.name_process_initialization_failures_cap).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_process_failures_limit)
				.withDescription(makeDesc(DuccUiConstants.desc_process_failures_limit,DuccUiConstants.exmp_process_failures_limit)).hasArg()
				.withLongOpt(DuccUiConstants.name_process_failures_limit).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_process_thread_count)
				.withDescription(makeDesc(DuccUiConstants.desc_process_thread_count,DuccUiConstants.exmp_process_thread_count)).hasArg()
				.withLongOpt(DuccUiConstants.name_process_thread_count).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_process_per_item_time_max)
				.withDescription(makeDesc(DuccUiConstants.desc_process_per_item_time_max,DuccUiConstants.exmp_process_per_item_time_max)).hasArg()
				.withLongOpt(DuccUiConstants.name_process_per_item_time_max).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_process_get_meta_time_max)
				.withDescription(makeDesc(DuccUiConstants.desc_process_get_meta_time_max,DuccUiConstants.exmp_process_get_meta_time_max)).hasArg()
				.withLongOpt(DuccUiConstants.name_process_get_meta_time_max).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_classpath_order)
				.withDescription(DuccUiConstants.desc_classpath_order).hasArg()
				.withLongOpt(DuccUiConstants.name_classpath_order).create());
		// </UIMA service only>
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_specification)
				.withDescription(DuccUiConstants.desc_specification).hasArg()
				.withLongOpt(DuccUiConstants.name_specification).create());

		options.addOption(OptionBuilder
                          .withArgName    (DuccUiConstants.parm_service_dependency)
                          .withDescription(makeDesc(DuccUiConstants.desc_service_dependency,DuccUiConstants.exmp_service_dependency))
                          .hasArgs        ()
                          .withValueSeparator(',')
                          .withLongOpt    (DuccUiConstants.name_service_dependency)
                          .create         ()
                          );

		options.addOption(OptionBuilder
                          .withLongOpt    (RegistrationOption.ServiceRequestEndpoint.decode()) 
                          .withDescription(RegistrationOption.ServiceRequestEndpoint.description()) 
                          .withArgName    (RegistrationOption.ServiceRequestEndpoint.argname())
                          .hasArg(true)
                          .create()
                          );

	}
	
    //**********
    
    private boolean has_writable_log_directory(Properties properties) {
		boolean retVal = true;
		String log_directory = properties.getProperty(JobRequestProperties.key_log_directory);
		File file = new File(log_directory);
		if(!file.isDirectory()) {
			duccMessageProcessor.err("not a directory: "+log_directory);
			retVal = false;
		}
		else if(!file.canWrite()) {
			duccMessageProcessor.err("not a writable directory: "+log_directory);
			retVal = false;
		}
		return retVal;
	}
    
	//**********
	
	private String[] required_options = {  };
	
	private boolean missing_required_options(Properties properties) {
		boolean retVal = false;
		for(int i=0; i<required_options.length; i++) {
			String required_option = required_options[i];
			if (properties.containsKey(required_option)) {
				duccMessageProcessor.err("missing required option: "+required_option);
				retVal = true;
			}
		}
		return retVal;
	}
	
	//**********

    private static final HashMap<String, List<String>> consumer = new HashMap<String, List<String>>(){
		private static final long serialVersionUID = 1L;

		{
            put("", Arrays.asList( ServiceRequestProperties.key_process_DD
            					   ));
        }
    };
    
    private static final String consumer_list = ServiceRequestProperties.key_process_DD
	   									   	  	;
    
	private boolean has_consumer(Properties properties) {
		boolean retVal = true;
		Iterator<Entry<String, List<String>>> iteratorA = consumer.entrySet().iterator();
		while(iteratorA.hasNext()) {
			boolean has_one = false;
			Entry<String, List<String>> entrySet = iteratorA.next();
			Iterator<String> iteratorB = entrySet.getValue().iterator();
			while(iteratorB.hasNext()) {
				String option = iteratorB.next();
				if (properties.containsKey(option)) {
					has_one = true;
					break;
				}
			}
			if(!has_one) {
				duccMessageProcessor.err("missing option, specify at least one of: "+consumer_list);
				retVal = false;
			}
		}
		return retVal;
	}
	
	//**********
    
    private static final HashMap<String, List<String>> mutually_exclusive_options = new HashMap<String, List<String>>(){
		private static final long serialVersionUID = 1L;

		{
            put(ServiceRequestProperties.key_service_type_uima, Arrays.asList( 	ServiceRequestProperties.key_service_type_other
																				));
            
            put(ServiceRequestProperties.key_service_type_other, Arrays.asList( ServiceRequestProperties.key_process_DD,
            																	ServiceRequestProperties.key_process_deployments_max,
            																	ServiceRequestProperties.key_process_initialization_failures_cap,
            																	ServiceRequestProperties.key_process_failures_limit,
            																	ServiceRequestProperties.key_process_thread_count,
            																	ServiceRequestProperties.key_process_per_item_time_max,
            																	ServiceRequestProperties.key_process_get_meta_time_max
																				));
        }
    };

	private boolean has_mutually_exclusive_options(Properties properties) {
		boolean retVal = false;
		Iterator<Entry<String, List<String>>> iteratorA = mutually_exclusive_options.entrySet().iterator();
		while(iteratorA.hasNext()) {
			Entry<String, List<String>> entrySet = iteratorA.next();
			if (properties.containsKey(entrySet.getKey())) {
				Iterator<String> iteratorB = entrySet.getValue().iterator();
				while(iteratorB.hasNext()) {
					String option = iteratorB.next();
					if (properties.containsKey(option)) {
						duccMessageProcessor.err("conflicting options: "+entrySet.getKey()+" and "+option);
						retVal = true;
					}
				}
			}
		}
		return retVal;
	}
		

    /**
     * - Verify that the minimum properties for a pop are here
     * - Verify no illegal properties for a pop
     */
    private boolean verifyPopProperties(Properties props)
    {
        String[] required = {
            "process_executable",
            "process_memory_size",
            "scheduling_class"
        };
        boolean answer = true;
        for ( String k : required ) {
            if ( ! props.containsKey(k) ) {
                System.out.println("Missing required property: " + k);
                answer = false;
            }
        }
        return answer;
    }

	//**********
	
//	private String getDuccProperty(String propertyName, String defaultValue) {
//		String propertyValue = defaultValue;
//		try {
//			String value = DuccPropertiesResolver.getInstance().getProperty(propertyName);
//			if(value != null) {
//				propertyValue = value;
//			}
//		}
//		catch(Throwable t) {
//			duccMessageProcessor.throwable(t);
//		}
//		return propertyValue;
//	}
	
    /*
     * resolve ${defaultBrokerURL} in service dependencies - must fail if resolution needed but can't resolve
     */
    boolean resolve_service_dependencies(String endpoint, Properties props)
    {
        if ( serviceDeploymentType == ServiceDeploymentType.other) return true;

        String deps = props.getProperty(ServiceRequestProperties.key_service_dependency);
        try {
            deps = DuccUiUtilities.resolve_service_dependencies(endpoint, deps, jvmargs);                
            if ( deps != null ) {
                props.setProperty(ServiceRequestProperties.key_service_dependency, deps);
            }
            return true;
        } catch ( Throwable t ) {
            duccMessageProcessor.err(t.getMessage());
            return false;
        }
    }

    protected void setServiceType(ServiceDeploymentType type)
    {
        this.serviceDeploymentType = type;
    }

    private ServiceDeploymentType getServiceType(ServiceRequestProperties serviceRequestProperties)
    {
        // some dude set this so we belive him - 
        if ( serviceDeploymentType != ServiceDeploymentType.unspecified ) return serviceDeploymentType;

        // if the service type is NOT set, then it has to be some kind of service, see if there's an  endpoint
        String service_endpoint = serviceRequestProperties.getProperty(RegistrationOption.ServiceRequestEndpoint.decode());

        // No end point, it HAS to be UIMA-AS and therefore requires a DD to be valid
        if (service_endpoint == null) {            
            String dd = (String) serviceRequestProperties.get(ServiceRequestProperties.key_process_DD);
            if ( dd != null ) {
                return ServiceDeploymentType.uima;
            } else {
                throw new IllegalArgumentException("Missing service endpoint and DD, cannot identify service type.");
            }
        }

        if ( service_endpoint.startsWith(ServiceType.Custom.decode()) ) {
            return ServiceDeploymentType.custom;
        }

        if ( service_endpoint.startsWith(ServiceType.UimaAs.decode()) ) {
            return ServiceDeploymentType.uima;
        }


        throw new IllegalArgumentException("Invalid service type in endpoint, must be " + ServiceType.UimaAs.decode() + " or " + ServiceType.Custom.decode() + ".");
    }

	//**********
	
	protected int help(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(DuccUiConstants.help_width);
		formatter.printHelp(DuccServiceSubmit.class.getName(), options);
		return DuccUiConstants.ERROR;
	}

	private boolean use_signature = false;
	
	@SuppressWarnings("unused")
	public int run(String[] args) throws Exception {
		ServiceRequestProperties serviceRequestProperties = new ServiceRequestProperties();

		/*
		 * parser is not thread safe?
		 */
		synchronized(DuccUi.class) {
			Options options = new Options();
			addOptions(options);
			/*
			for (String s : args) {
				System.out.println("arg |"+s+"|");
			}
			*/
			CommandLineParser parser = new PosixParser();
			CommandLine commandLine = parser.parse(options, args);
			/*
			 * give help & exit when requested
			 */
			if (commandLine.hasOption(DuccUiConstants.name_help)) {
				return help(options);
			}
			if(commandLine.getOptions().length == 0) {
				return help(options);
			}

			/*
			 * require DUCC_HOME 
			 */
			String ducc_home_key = "DUCC_HOME";
			String ducc_home = System.getenv(ducc_home_key);
			if(ducc_home == null) {
				duccMessageProcessor.err("missing required environment variable: "+ducc_home_key);
				return DuccUiConstants.ERROR;
			}

			/*
			 * detect duplicate options
			 */
			if (DuccUiUtilities.duplicate_options(duccMessageProcessor, commandLine)) {
				return DuccUiConstants.ERROR;
			}
						
			/*
			 * marshal user
			 */
			String user = DuccUiUtilities.getUser();
			serviceRequestProperties.setProperty(ServiceSpecificationProperties.key_user, user);
			String property = DuccPropertiesResolver.getInstance().getProperty(DuccPropertiesResolver.ducc_signature_required);
			if(property != null) {
				String signatureRequiredProperty = property.trim().toLowerCase();
				if(signatureRequiredProperty.equals("on")) {
					Crypto crypto = new Crypto(System.getProperty("user.home"));
					byte[] cypheredMessage = crypto.encrypt(user);
					serviceRequestProperties.put(ServiceSpecificationProperties.key_signature, cypheredMessage);
					use_signature = true;
				}
			}
			/*
			 * marshal command line options into properties
			 */
			Option[] optionList = commandLine.getOptions();
			// pass 1
			for (int i=0; i<optionList.length; i++) {
				Option option = optionList[i];
				String name = option.getLongOpt();
				if(name.equals(SpecificationProperties.key_specification)) {
					File file = new File(option.getValue());
					FileInputStream fis = new FileInputStream(file);
					serviceRequestProperties.load(fis);
				}
			}

            jvmarg_string = (String) serviceRequestProperties.get(ServiceRequestProperties.key_process_jvm_args);
            jvmargs = DuccUiUtilities.jvmArgsToProperties(jvmarg_string);
            DuccUiUtilities.resolvePropertiesPlaceholders(serviceRequestProperties, jvmargs);

			// trim
			DuccUiUtilities.trimProperties(serviceRequestProperties);
			// pass 2
			for (int i=0; i<optionList.length; i++) {
				Option option = optionList[i];
				String name = option.getLongOpt();

				String value = null;
                if ( option.hasArgs() ) {        // if multiple args, make into blank-delimited string for the props file
                    String[] arglist = commandLine.getOptionValues(name);
                    int len = arglist.length;
                    StringBuffer sb = new StringBuffer();
                    for ( int ii = 0; ii < len; ii++ ) {
                        String a = arglist[ii].trim();
                        if ( a.equals("") ) continue;
                        sb.append(a);
                        if ( ii < (len-1) ) {
                            sb.append(", ");
                        }
                    }
                    value = sb.toString();
                } else {
                    value = option.getValue();
                }

				if(value == null) {
					value = "";
				}

                // System.out.println(name + " " + value);
				name = trimmer(name);
				value = trimmer(value);
				serviceRequestProperties.setProperty(name, value);
			}
		}
        
		/*
		 * employ default log directory if not specified
		 */
		String log_directory = serviceRequestProperties.getProperty(ServiceRequestProperties.key_log_directory);
		if(log_directory == null) {
			// no log directory was specified - default to user's home + "/ducc/logs"
			log_directory = System.getProperty("user.home")+IDucc.userLogsSubDirectory;
		}
		else {
			if(log_directory.startsWith(File.separator)) {
			// absolute log directory was specified
			}
			else {
				// relative log directory was specified - default to user's home + relative directory
				if(log_directory.endsWith(File.separator)) {
					log_directory = System.getProperty("user.home")+log_directory;
				}
				else {
					log_directory = System.getProperty("user.home")+File.separator+log_directory;
				}
			}
		}
		serviceRequestProperties.setProperty(ServiceRequestProperties.key_log_directory,log_directory);
        /*
         * make sure the logdir is actually legal.
         */
        if (!has_writable_log_directory(serviceRequestProperties)) {
            return DuccUiConstants.ERROR;
        }


		// tack on "services" or "processes" to complete logging directory
        String log_extension = "services";
        if ( serviceDeploymentType == ServiceDeploymentType.other ) {
            log_extension = "processes";
        }
		if(log_directory.endsWith(File.separator)) {
			log_directory = log_directory + log_extension;
		}
		else {
			log_directory = log_directory + File.separator + log_extension;
		}
		serviceRequestProperties.setProperty(ServiceRequestProperties.key_log_directory,log_directory);

		/*
		 * employ default working directory if not specified
		 */
		String working_directory = serviceRequestProperties.getProperty(ServiceRequestProperties.key_working_directory);
		if(working_directory == null) {
			working_directory = System.getProperty("user.dir");
			serviceRequestProperties.setProperty(ServiceRequestProperties.key_working_directory,working_directory);
		}
		/*
		 * employ default process classpath if not specified
		 */
		String process_classpath = serviceRequestProperties.getProperty(ServiceRequestProperties.key_process_classpath);
		if(process_classpath == null) {
			process_classpath = System.getProperty("java.class.path");
			serviceRequestProperties.setProperty(ServiceRequestProperties.key_process_classpath,process_classpath);
		}
		
// 		if(serviceRequestProperties.containsKey(ServiceRequestProperties.key_service_type_custom)) {
// 			serviceDeploymentType = ServiceDeploymentType.custom;
// 		}
// 		else if(serviceRequestProperties.containsKey(ServiceRequestProperties.key_service_type_other)) {
// 			serviceDeploymentType = ServiceDeploymentType.other;
// 		}
// 		else {
// 			serviceRequestProperties.put(ServiceRequestProperties.key_service_type_uima, "");
// 			//serviceDeploymentType = ServiceDeploymentType.uima;
// 		}

        try {
            serviceDeploymentType = getServiceType(serviceRequestProperties);
        } catch ( Throwable t ) {
            System.out.println(t.getMessage());
            return DuccUiConstants.ERROR;
        }
        String service_endpoint = serviceRequestProperties.getProperty(RegistrationOption.ServiceRequestEndpoint.decode());
		switch(serviceDeploymentType) {
            case uima:
                serviceRequestProperties.put(ServiceRequestProperties.key_service_type_uima, "");
                if(service_endpoint == null) {
                    // A null endpoint means it MUST be UimaAs and we are going to derive it.  Otherwise it's the user's responsibility to
                    // have it set correctly, because really can't tell.                    
                    //
                    // The service endpoint is extracted from the DD. It is for internal use only, not publicly settable or documented.
                    //
                    try {
                        String dd = (String) serviceRequestProperties.get(ServiceRequestProperties.key_process_DD);
                        String wd = (String) serviceRequestProperties.get(ServiceRequestProperties.key_working_directory);
                        //System.err.println("DD: " + dd);
                        //System.err.println("WD: " + wd);
                        //System.err.println("jvmargs: " + jvmarg_string);
                        service_endpoint = DuccUiUtilities.getEndpoint(wd, dd, jvmargs);
                        serviceRequestProperties.put(ServiceRequestProperties.key_service_request_endpoint, service_endpoint);
                        if(serviceRequestProperties.containsKey(DuccUiConstants.name_debug)) {
                            duccMessageProcessor.out("service_endpoint:"+" "+service_endpoint);
                        }
                    } catch ( IllegalArgumentException e ) {
                        duccMessageProcessor.exception(e);
                        duccMessageProcessor.err("Cannot read/process DD descriptor for endpoint: " + e.getMessage());
                        return DuccUiConstants.ERROR;
                    }
                } else {
                    serviceRequestProperties.put(ServiceRequestProperties.key_service_request_endpoint, service_endpoint);
                }
                break;

            case custom:
                serviceRequestProperties.put(ServiceRequestProperties.key_service_type_custom, "");
                if ( service_endpoint == null ) {
                    duccMessageProcessor.err("Missing endpoint for CUSTOM service.");
                    return DuccUiConstants.ERROR;
                } else {
                    serviceRequestProperties.put(ServiceRequestProperties.key_service_request_endpoint, service_endpoint);
                }
                break;

            case other:
                if ( ! verifyPopProperties(serviceRequestProperties) ) {
                    // called method emits messages
                    return DuccUiConstants.ERROR;
                }
                serviceRequestProperties.put(ServiceRequestProperties.key_service_type_other, "");               
                break;

            case unspecified:
                // messages are  emitted in getServiceType
                return DuccUiConstants.ERROR;
        }

        if ( ! resolve_service_dependencies(service_endpoint, serviceRequestProperties) ) {            
            return DuccUiConstants.ERROR;
        }
        
		if(serviceRequestProperties.containsKey(DuccUiConstants.name_debug)) {
			serviceRequestProperties.dump();
		}

        // TODO: Need to rework these checks
        if ( false ) {
            /*
             * check for required options
             */
            if (missing_required_options(serviceRequestProperties)) {
                return DuccUiConstants.ERROR;
            }
            /*
             * check for mutually exclusive options
             */
            if (has_mutually_exclusive_options(serviceRequestProperties)) {
                return DuccUiConstants.ERROR;
            }
            /*
             * check for minimum set of options
             */
            switch(serviceDeploymentType) {
            case uima:
                if (!has_consumer(serviceRequestProperties)) {
					return DuccUiConstants.ERROR;
                }
                break;
            }
        }
	
        /*
		 * set DUCC_LD_LIBRARY_PATH in process environment
		 */
		if (!DuccUiUtilities.ducc_environment(duccMessageProcessor, serviceRequestProperties, ServiceRequestProperties.key_process_environment)) {
			return DuccUiConstants.ERROR;
		}

		/*
		 * identify invoker
		 */
		serviceRequestProperties.setProperty(ServiceRequestProperties.key_submitter_pid_at_host, ManagementFactory.getRuntimeMXBean().getName());
		
        boolean missingValue = false;
        Set<Object> keys = serviceRequestProperties.keySet();
        for(Object key : keys) {
        	if(ServiceRequestProperties.keys_requiring_values.contains(key)) {
        		Object oValue = serviceRequestProperties.get(key);
        		if(oValue == null) {
        			duccMessageProcessor.err("missing value for: "+key);
        			missingValue = true;
        		}
        		else if(oValue instanceof String) {
        			String sValue = (String)oValue;
        			if(sValue.trim().length() < 1) {
            			duccMessageProcessor.err("missing value for: "+key);
            			missingValue = true;
            		}
        		}
        		
        	}
        }
        if(missingValue) {
        	return DuccUiConstants.ERROR;
        }
		
		/*
		 * send to JM & get reply
		 */
//		CamelContext context = new DefaultCamelContext();
//		ActiveMQComponent amqc = ActiveMQComponent.activeMQComponent(broker);
//		String jmsProvider = DuccPropertiesResolver.getInstance().getProperty(DuccPropertiesResolver.ducc_jms_provider);
//        context.addComponent(jmsProvider, amqc);
//        context.start();
//        DuccEventDispatcher duccEventDispatcher;
//        duccEventDispatcher = new DuccEventDispatcher(context,endpoint);
        
        String port = 
                DuccPropertiesResolver.
                  getInstance().
                    getProperty(DuccPropertiesResolver.ducc_orchestrator_http_port);
        if ( port == null ) {
          throw new DuccRuntimeException("Unable to Submit a Job. Ducc Orchestrator HTTP Port Not Defined. Add ducc.orchestrator.http.port ducc.properties");
        }
        String orNode = 
                DuccPropertiesResolver.
                  getInstance().
                    getProperty(DuccPropertiesResolver.ducc_orchestrator_node);
        if ( orNode == null ) {
          throw new DuccRuntimeException("Unable to Submit a Job. Ducc Orchestrator Node Not Defined. Add ducc.orchestrator.node to ducc.properties");
        }
        
        String targetUrl = "http://"+orNode+":"+port+"/or";
        DuccEventHttpDispatcher duccEventDispatcher = new DuccEventHttpDispatcher(targetUrl);        
        SubmitServiceDuccEvent submitServiceDuccEvent = new SubmitServiceDuccEvent();
        submitServiceDuccEvent.setProperties(serviceRequestProperties);
        DuccEvent duccRequestEvent = submitServiceDuccEvent;
        DuccEvent duccReplyEvent = null;
        SubmitServiceReplyDuccEvent submitServiceReplyDuccEvent = null;
        try {
        	duccReplyEvent = duccEventDispatcher.dispatchAndWaitForDuccReply(duccRequestEvent);
        }
        finally {
          duccEventDispatcher.close();
        	//context.stop();
        }
        /*
         * process reply
         */
        submitServiceReplyDuccEvent = (SubmitServiceReplyDuccEvent) duccReplyEvent;
        int retVal = 0;
        Properties properties = submitServiceReplyDuccEvent.getProperties();
        @SuppressWarnings("unchecked")
		ArrayList<String> value_submit_warnings = (ArrayList<String>) properties.get(ServiceSpecificationProperties.key_submit_warnings);
        if(value_submit_warnings != null) {
        	duccMessageProcessor.out("Service"+" "+"warnings:");
        	Iterator<String> reasons = value_submit_warnings.iterator();
        	while(reasons.hasNext()) {
        		duccMessageProcessor.out(reasons.next());
        	}
        }
        @SuppressWarnings("unchecked")
		ArrayList<String> value_submit_errors = (ArrayList<String>) properties.get(ServiceSpecificationProperties.key_submit_errors);
        if(value_submit_errors != null) {
        	duccMessageProcessor.out("Service"+" "+"errors:");
        	Iterator<String> reasons = value_submit_errors.iterator();
        	while(reasons.hasNext()) {
        		duccMessageProcessor.out(reasons.next());
        	}
	        retVal = DuccUiConstants.ERROR;
        }
        String serviceId = "?";
        if(retVal == DuccUiConstants.ERROR) {
        	duccMessageProcessor.out("Service"+" "+"not"+" "+"submitted");
        }
        else {
        	serviceId = submitServiceReplyDuccEvent.getProperties().getProperty(ServiceRequestProperties.key_id);
        	saveServiceSpec(serviceId, serviceRequestProperties);
        	duccMessageProcessor.out("Service"+" "+serviceId+" "+"submitted");
        	/*
        	if(serviceRequestProperties.containsKey(DuccUiConstants.name_wait_for_completion)) {
        		try {
        			ArrayList<String> arrayList = new ArrayList<String>();
        			arrayList.add("--"+DuccUiConstants.name_service_id);
        			arrayList.add(serviceId);
        			arrayList.add("--"+DuccUiConstants.name_service_broker);
        			arrayList.add(broker);
        			if(serviceRequestProperties.containsKey(DuccUiConstants.name_debug)) {
        				arrayList.add("--"+DuccUiConstants.name_debug);
        			}
        			if(serviceRequestProperties.containsKey(DuccUiConstants.name_timestamp)) {
        				arrayList.add("--"+DuccUiConstants.name_timestamp);
        			}
        			if(serviceRequestProperties.containsKey(DuccUiConstants.name_submit_cancel_service_on_interrupt)) {
        				arrayList.add("--"+DuccUiConstants.name_monitor_cancel_service_on_interrupt);
        			}
        			String[] argList = arrayList.toArray(new String[0]);
        			DuccServiceMonitor duccServiceMonitor = new DuccServiceMonitor();
        			retVal = duccServiceMonitor.run(argList);
        		} catch (Exception e) {
        			e.printStackTrace();
        			retVal = DuccUiConstants.ERROR;
        		}
        	}
        	*/
        }
		return retVal;
	}
	
	private void saveServiceSpec(String serviceId, ServiceRequestProperties serviceRequestProperties) {
		try {
			String directory = serviceRequestProperties.getProperty(ServiceRequestProperties.key_log_directory)+File.separator+serviceId+File.separator;
			IOHelper.mkdirs(directory);
			String fileName = directory+"service-specification.properties";
			String comments = null;
			FileOutputStream fos = null;
			OutputStreamWriter out = null;
			fos = new FileOutputStream(fileName);
			out = new OutputStreamWriter(fos);

			if(use_signature) {
				String key = SpecificationProperties.key_signature;
				Object value = serviceRequestProperties.remove(key);
				serviceRequestProperties.store(out, comments);
				serviceRequestProperties.put(key, value);
			}
			else {
				serviceRequestProperties.store(out, comments);
			}

			out.close();
			fos.close();
		}
		catch(Throwable t) {
			duccMessageProcessor.throwable(t);
		}
	}
	
	public static void main(String[] args) {
		try {
			DuccServiceSubmit duccServiceSubmit = new DuccServiceSubmit();
			int rc = duccServiceSubmit.run(args);
            System.exit(rc == 0 ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
            System.exit(1);
		}
	}
	
}
