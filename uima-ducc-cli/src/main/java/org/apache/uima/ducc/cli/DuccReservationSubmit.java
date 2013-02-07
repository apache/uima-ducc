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
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Iterator;
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
import org.apache.uima.ducc.common.crypto.Crypto;
import org.apache.uima.ducc.common.exception.DuccRuntimeException;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.transport.dispatcher.DuccEventHttpDispatcher;
import org.apache.uima.ducc.transport.event.DuccEvent;
import org.apache.uima.ducc.transport.event.SubmitReservationDuccEvent;
import org.apache.uima.ducc.transport.event.SubmitReservationReplyDuccEvent;
import org.apache.uima.ducc.transport.event.cli.ReservationRequestProperties;
import org.apache.uima.ducc.transport.event.cli.ReservationSpecificationProperties;
import org.apache.uima.ducc.transport.event.cli.SpecificationProperties;


/**
 * Submit a DUCC reservation
 */

public class DuccReservationSubmit extends DuccUi {
	
	private IDuccMessageProcessor duccMessageProcessor = new DuccMessage();
	private String nodeList = "";
	private String reservationId = "";
	
	public DuccReservationSubmit() {
	}
	
	public DuccReservationSubmit(IDuccMessageProcessor duccMessageProcessor) {
		this.duccMessageProcessor = duccMessageProcessor;
	}

	@SuppressWarnings("static-access")
	private void addOptions(Options options) {
		options.addOption(OptionBuilder
				.withDescription(DuccUiConstants.desc_help).hasArg(false)
				.withLongOpt(DuccUiConstants.name_help).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_description)
				.withDescription(makeDesc(DuccUiConstants.desc_description,DuccUiConstants.exmp_description)).hasArg()
				.withLongOpt(DuccUiConstants.name_description).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_reservation_scheduling_class)
				.withDescription(makeDesc(DuccUiConstants.desc_reservation_scheduling_class,DuccUiConstants.exmp_reservation_scheduling_class)).hasArg()
				.withLongOpt(DuccUiConstants.name_reservation_scheduling_class).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_number_of_instances)
				.withDescription(makeDesc(DuccUiConstants.desc_number_of_instances,DuccUiConstants.exmp_number_of_instances)).hasArg()
				.withLongOpt(DuccUiConstants.name_number_of_instances).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_instance_memory_size)
				.withDescription(makeDesc(DuccUiConstants.desc_instance_memory_size,DuccUiConstants.exmp_instance_memory_size)).hasArg()
				.withLongOpt(DuccUiConstants.name_instance_memory_size).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_specification)
				.withDescription(DuccUiConstants.desc_specification).hasArg()
				.withLongOpt(DuccUiConstants.name_specification).create());
	}

	private String[] required_options = { };
	
	private boolean missing_required_options(CommandLine commandLine) {
		boolean retVal = false;
		for(int i=0; i<required_options.length; i++) {
			String required_option = required_options[i];
			if (!commandLine.hasOption(required_option)) {
				duccMessageProcessor.err("missing required option: "+required_option);
				retVal = true;
			}
		}
		return retVal;
	}
	
	protected int help(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(DuccUiConstants.help_width);
		formatter.printHelp(DuccReservationSubmit.class.getName(), options);
		return DuccUiConstants.ERROR;
	}
	
	public int run(String[] args) throws Exception {
		ReservationRequestProperties reservationRequestProperties = new ReservationRequestProperties();
		/*
		 * parser is not thread safe?
		 */
		synchronized(DuccUi.class) {
			Options options = new Options();
			addOptions(options);
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
			 * check for required options
			 */
			if (missing_required_options(commandLine)) {
				return DuccUiConstants.ERROR;
			}
			/*
			 * marshal user
			 */
			String user = DuccUiUtilities.getUser();
			reservationRequestProperties.setProperty(ReservationSpecificationProperties.key_user, user);
			String property = DuccPropertiesResolver.getInstance().getProperty(DuccPropertiesResolver.ducc_signature_required);
			if(property != null) {
				String signatureRequiredProperty = property.trim().toLowerCase();
				if(signatureRequiredProperty.equals("on")) {
					Crypto crypto = new Crypto(System.getProperty("user.home"));
					byte[] cypheredMessage = crypto.encrypt(user);
					reservationRequestProperties.put(ReservationSpecificationProperties.key_signature, cypheredMessage);
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
					reservationRequestProperties.load(fis);
				}
			}
			// trim
			DuccUiUtilities.trimProperties(reservationRequestProperties);
			// pass 2
			for (int i=0; i<optionList.length; i++) {
				Option option = optionList[i];
				String name = option.getLongOpt();
				String value = option.getValue();
				name = trimmer(name);
				value = trimmer(value);
				reservationRequestProperties.setProperty(name, value);
			}
		}
		/*
		 * employ default broker/endpoint if not specified
		 */
		String broker = reservationRequestProperties.getProperty(ReservationRequestProperties.key_service_broker);
		if(broker == null) {
			broker = DuccUiUtilities.buildBrokerUrl();
		}
		String endpoint = reservationRequestProperties.getProperty(ReservationRequestProperties.key_service_endpoint);
		if(endpoint == null) {
			endpoint = DuccPropertiesResolver.getInstance().getProperty(DuccPropertiesResolver.ducc_jms_provider)
				     + ":"
				     + DuccPropertiesResolver.getInstance().getProperty(DuccPropertiesResolver.ducc_orchestrator_request_endpoint_type)
				     + ":"
				     + DuccPropertiesResolver.getInstance().getProperty(DuccPropertiesResolver.ducc_orchestrator_request_endpoint)
				     ;
		}
		/*
		 * identify invoker
		 */
		reservationRequestProperties.setProperty(ReservationRequestProperties.key_submitter_pid_at_host, ManagementFactory.getRuntimeMXBean().getName());
		
        boolean missingValue = false;
        Set<Object> keys = reservationRequestProperties.keySet();
        for(Object key : keys) {
        	if(ReservationRequestProperties.keys_requiring_values.contains(key)) {
        		Object oValue = reservationRequestProperties.get(key);
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
		 * send to Orchestrator & get reply
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
        SubmitReservationDuccEvent submitReservationDuccEvent = new SubmitReservationDuccEvent();
        submitReservationDuccEvent.setProperties(reservationRequestProperties);
        DuccEvent duccRequestEvent = submitReservationDuccEvent;
        DuccEvent duccReplyEvent = null;
        SubmitReservationReplyDuccEvent submitReservationReplyDuccEvent = null;
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
        submitReservationReplyDuccEvent = (SubmitReservationReplyDuccEvent) duccReplyEvent;
        int retVal = 0;
        Properties properties = submitReservationReplyDuccEvent.getProperties();
        @SuppressWarnings("unchecked")
		ArrayList<String> value_submit_warnings = (ArrayList<String>) properties.get(ReservationSpecificationProperties.key_submit_warnings);
        if(value_submit_warnings != null) {
        	duccMessageProcessor.out("Reservation"+" "+"warnings:");
        	Iterator<String> reasons = value_submit_warnings.iterator();
        	while(reasons.hasNext()) {
        		duccMessageProcessor.out(reasons.next());
        	}
        }
        @SuppressWarnings("unchecked")
		ArrayList<String> value_submit_errors = (ArrayList<String>) properties.get(ReservationSpecificationProperties.key_submit_errors);
        if(value_submit_errors != null) {
        	duccMessageProcessor.out("Reservation"+" "+"errors:");
        	Iterator<String> reasons = value_submit_errors.iterator();
        	while(reasons.hasNext()) {
        		duccMessageProcessor.out(reasons.next());
        	}
	        retVal = DuccUiConstants.ERROR;
        }
        if(retVal == DuccUiConstants.ERROR) {
        	duccMessageProcessor.out("Reservation"+" "+"not"+" "+"submitted");
        	return DuccUiConstants.ERROR;
        }
        else {
        	reservationId = submitReservationReplyDuccEvent.getProperties().getProperty(ReservationRequestProperties.key_id);
        	nodeList = submitReservationReplyDuccEvent.getProperties().getProperty(ReservationRequestProperties.key_node_list);
        	duccMessageProcessor.out("Reservation"+" "+reservationId+" "+"submitted");
        	boolean success = nodeList.trim().length() > 0;
        	if(success) {
        		duccMessageProcessor.out("resID = "+reservationId);
        		duccMessageProcessor.out("nodes: "+nodeList);
        	}
        	else {
        		duccMessageProcessor.out("Resources Unavailable");
        	}
            return new Integer(reservationId);
        }
	}
	
	@Deprecated
	public String getReservationId() {
		  return this.reservationId;
	}
	
	@Deprecated
	public String[] getHosts() {
		  return this.nodeList.split(" ");
	}
	
	public static void main(String[] args) {
		try {
			DuccReservationSubmit duccReservationSubmit = new DuccReservationSubmit();
			int rc = duccReservationSubmit.run(args);
            System.exit(rc == 0 ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
            System.exit(1);
		}
	}
	
}
