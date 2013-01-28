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
import org.apache.uima.ducc.transport.event.CancelReservationDuccEvent;
import org.apache.uima.ducc.transport.event.CancelReservationReplyDuccEvent;
import org.apache.uima.ducc.transport.event.DuccEvent;
import org.apache.uima.ducc.transport.event.cli.ReservationReplyProperties;
import org.apache.uima.ducc.transport.event.cli.ReservationRequestProperties;
import org.apache.uima.ducc.transport.event.cli.ReservationSpecificationProperties;


/**
 * Cancel a DUCC reservation
 */

public class DuccReservationCancel extends DuccUi {
	
	private IDuccMessageProcessor duccMessageProcessor = new DuccMessage();
	
	public DuccReservationCancel() {
	}
	
	public DuccReservationCancel(IDuccMessageProcessor duccMessageProcessor) {
		this.duccMessageProcessor = duccMessageProcessor;
	}
	
	@SuppressWarnings("static-access")
	private void addOptions(Options options) {
		options.addOption(OptionBuilder
				.withDescription(DuccUiConstants.desc_help).hasArg(false)
				.withLongOpt(DuccUiConstants.name_help).create());
		options.addOption(OptionBuilder
				.withDescription(DuccUiConstants.desc_role_administrator).hasArg(false)
				.withLongOpt(DuccUiConstants.name_role_administrator).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_reservation_id)
				.withDescription(makeDesc(DuccUiConstants.desc_reservation_id,DuccUiConstants.exmp_reservation_id)).hasArg()
				.withLongOpt(DuccUiConstants.name_reservation_id).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_service_broker)
				.withDescription(makeDesc(DuccUiConstants.desc_service_broker,DuccUiConstants.exmp_service_broker)).hasArg()
				.withLongOpt(DuccUiConstants.name_service_broker).create());
		options.addOption(OptionBuilder
				.withArgName(DuccUiConstants.parm_service_endpoint)
				.withDescription(makeDesc(DuccUiConstants.desc_service_endpoint,DuccUiConstants.exmp_service_endpoint)).hasArg()
				.withLongOpt(DuccUiConstants.name_service_endpoint).create());
	}
	
	protected int help(Options options) {
		HelpFormatter formatter = new HelpFormatter();
		formatter.setWidth(DuccUiConstants.help_width);
		formatter.printHelp(DuccReservationCancel.class.getName(), options);
		return 1;
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
				return 1;
			}
			/*
			 * detect duplicate options
			 */
			if (DuccUiUtilities.duplicate_options(duccMessageProcessor, commandLine)) {
				return 1;
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
			for (int i=0; i<optionList.length; i++) {
				Option option = optionList[i];
				String name = option.getLongOpt();
				String value = option.getValue();
				name = trimmer(name);
				value = trimmer(value);
				if(value == null) {
					value = "";
				}
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
		 
		    CancelReservationDuccEvent cancelReservationDuccEvent = new CancelReservationDuccEvent();
        cancelReservationDuccEvent.setProperties(reservationRequestProperties);
        DuccEvent duccRequestEvent = cancelReservationDuccEvent;
        DuccEvent duccReplyEvent = null;
        CancelReservationReplyDuccEvent cancelReservationReplyDuccEvent = null;
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
        cancelReservationReplyDuccEvent = (CancelReservationReplyDuccEvent) duccReplyEvent;
        // TODO handle null & rejected possibilities here
    	String reservationId = cancelReservationReplyDuccEvent.getProperties().getProperty(ReservationReplyProperties.key_id);
    	String msg = cancelReservationReplyDuccEvent.getProperties().getProperty(ReservationReplyProperties.key_message);
    	duccMessageProcessor.out("Reservation"+" "+reservationId+" "+msg);
		return 0;
	}
	
	public static void main(String[] args) {
		try {
			DuccReservationCancel duccReservationCancel = new DuccReservationCancel();
			int rc = duccReservationCancel.run(args);
            System.exit(rc == 0 ? 0 : 1);
		} catch (Exception e) {
			e.printStackTrace();
            System.exit(1);
		}
	}
	
}
