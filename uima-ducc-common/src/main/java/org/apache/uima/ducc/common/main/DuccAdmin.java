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
package org.apache.uima.ducc.common.main;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import org.apache.activemq.camel.component.ActiveMQComponent;
import org.apache.camel.CamelContext;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.dataformat.xstream.XStreamDataFormat;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.DefaultClassResolver;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.uima.ducc.common.admin.event.DuccAdminEvent;
import org.apache.uima.ducc.common.admin.event.DuccAdminEventKill;
import org.apache.uima.ducc.common.admin.event.DuccAdminEventStopMetrics;
import org.apache.uima.ducc.common.authentication.BrokerCredentials;
import org.apache.uima.ducc.common.authentication.BrokerCredentials.Credentials;
import org.apache.uima.ducc.common.component.AbstractDuccComponent;
import org.apache.uima.ducc.common.launcher.ssh.DuccRemoteLauncher;
import org.apache.uima.ducc.common.launcher.ssh.DuccRemoteLauncher.ProcessCompletionCallback;
import org.apache.uima.ducc.common.launcher.ssh.DuccRemoteLauncher.ProcessCompletionResult;
import org.apache.uima.ducc.common.utils.Utils;

import com.thoughtworks.xstream.XStream;

/**
 * This class implements support for sending Administrative commands to Ducc
 * processes. Usage:
 * 
 * DuccAdmin -killAll - stops all Ducc components
 * DuccAdmin -quiesceAgents <node1,node2..>- quiesces named nodes
 */
public class DuccAdmin extends AbstractDuccComponent implements
		ProcessCompletionCallback {
	public static final String FileSeparator = System
			.getProperty("file.separator");

	public static enum DuccCommands {
		killAll, startAgents, quiesceAgents
	};

	private String brokerUrl;
	private ProducerTemplate pt;
	private String targetEndpoint;
	private CommandLine commandLine;

	public DuccAdmin(CamelContext context) {
		super("DuccServiceReaper", context);
		try {
			// Load ducc properties file and enrich System properties. It
			// supports
			// overrides for entries in ducc properties file. Any key in the
			// ducc
			// property file can be overriden with -D<key>=<value>
			loadProperties(DuccService.DUCC_PROPERTY_FILE);
			// fetch the broker URL from ducc.properties
			this.brokerUrl = System.getProperty("ducc.broker.url");
			try {
				String brokerCredentialsFile = 
						System.getProperty("ducc.broker.credentials.file");
				// fetch the admin endpoint from the ducc.properties where
				// the admin events will be sent by the DuccServiceReaper
				targetEndpoint = System.getProperty("ducc.admin.endpoint");
				System.out.println("+++ Activating JMS Component for Endpoint:"
						+ targetEndpoint + " Broker:" + brokerUrl);
				
				ActiveMQComponent duccAMQComponent = new ActiveMQComponent(context);
			    duccAMQComponent.setBrokerURL(brokerUrl);
				
//				context.addComponent("activemq",
//						ActiveMQComponent.activeMQComponent(brokerUrl));
			    
			    if ( brokerCredentialsFile != null && brokerCredentialsFile.length() > 0 ) {
			    	String path ="";
			    	try {
			    		  Utils.findDuccHome();  // add DUCC_HOME to System.properties
					      path = Utils.resolvePlaceholderIfExists(brokerCredentialsFile, System.getProperties());
				    	  Credentials credentials = BrokerCredentials.get(path);
						  if ( credentials.getUsername() != null && credentials.getPassword() != null ) {
							duccAMQComponent.setUserName(credentials.getUsername());
							duccAMQComponent.setPassword(credentials.getPassword());
				 		  }   
			    	} catch(FileNotFoundException e) {
						System.out.println("DuccAdmin Failed - File Not Found:"+path+" Broker Credentials File:"+brokerCredentialsFile);
						System.exit(-1);
			    	}
			    }
				context.addComponent("activemq",duccAMQComponent);
				this.pt = context.createProducerTemplate();
			} catch( Throwable exx) {
				System.out.println("DuccAdmin Failed:"+exx);
				System.exit(-1);
			}

		} catch (Exception e) {
			System.out
					.println("DuccAdmin was not able to load properties from "
							+ (String) System
									.getProperty(DuccService.DUCC_PROPERTY_FILE));
			System.exit(-1);
		}
	}

	/**
	 * Sends the DuccAdminEvent (serialized as String) to the admin endpoint
	 * 
	 * @param serializedEvent
	 *            - serialized DuccAdminEvent
	 * @throws Exception
	 */
	public void dispatch(String serializedEvent) throws Exception {
		// this is a one-way send. Reply is not expected
		pt.sendBody(targetEndpoint, serializedEvent);
	}

	/**
	 * Serializes DuccAdminEvent using XStream
	 * 
	 * @param event
	 *            - instance of DuccAdminEvent
	 * @return - serialized DuccAdminEvent
	 */
	public String serializeAdminEvent(DuccAdminEvent event) {
		XStreamDataFormat xStreamDataFormat = new XStreamDataFormat();
		XStream xStream = xStreamDataFormat
				.getXStream(new DefaultClassResolver());
		return xStream.toXML(event);
	}

	public Options getPosixOptions() {
		final Options posixOptions = new Options();
		posixOptions.addOption(DuccCommands.killAll.name(), false,
				"Kill All Ducc Processes");

		@SuppressWarnings("static-access")
		Option startAgentsOption = OptionBuilder
				.hasArgs(2)
				.withDescription(
						"starting agents defined in arg1 using command defined in arg2")
				.create("startAgents");
		posixOptions.addOption(startAgentsOption);

		@SuppressWarnings("static-access")
    Option quiesceAgentsOption = OptionBuilder
        .hasArgs(1)
        .withDescription(
            "quiescing agents defined in arg1")
        .create("quiesceAgents");
    posixOptions.addOption(quiesceAgentsOption);
    
		return posixOptions;
	}

	public void parseWithPosixParser(final String[] args) {
		final CommandLineParser cmdLinePosixParser = new PosixParser();
		final Options posixOptions = getPosixOptions();
		try {
			commandLine = cmdLinePosixParser.parse(posixOptions, args);
		} catch (ParseException parseException) {
			System.err
					.println("Exception while parsing using commandling with PosixParser:\n"
							+ parseException.getMessage());
		}
	}

	/**
	 * This method facilitates complete DUCC system shutdown by sending
	 * DuccEventKill event to all Ducc components. Each Ducc component has
	 * special purpose JMS channel where it listens for Ducc Admin events. When
	 * an event is received the component shuts itself down.
	 * 
	 * @throws Exception
	 */
	private void killAll() throws Exception {
		// send kill event to all Ducc components via Ducc Admin Channel. This
		// call is
		// non-blocking
		dispatch(serializeAdminEvent(new DuccAdminEventKill()));
		System.out.println("DuccAdmin sent Kill to all Ducc processes ...");
	}

	private void quiesceAgents(String nodes) throws Exception {
    dispatch(serializeAdminEvent(new DuccAdminEventStopMetrics(nodes)));
    System.out.println("DuccAdmin sent Quiesce request to Ducc Agents ...");
	}
	/**
	 * Return contents of the provided command file.
	 * 
	 * @param commandFile - file containing command to run
	 * @return - contents of the command file
	 * 
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	private String readCommand(String commandFile)
			throws FileNotFoundException, IOException {
		File file = new File(commandFile);
		InputStream in = new FileInputStream(file);
		// Get the size of the file
		long length = file.length();
		// Create the byte array to hold a command
		byte[] bytes = new byte[(int) length];

		// Read in the bytes
		int offset = 0;
		int numRead = 0;
		while (offset < bytes.length
				&& (numRead = in.read(bytes, offset, bytes.length - offset)) >= 0) {
			offset += numRead;
		}
		in.close();
		return new String(bytes);
	}
	/**
	 * Launches agents on nodes identified in nodesFile. The commandFile
	 * contains a command that will be used to execute the agent.
	 */
	private void startAgents(String nodesFile, String commandFile) {
		InputStream in = null;
		try {
			//	get the command line to start agent process 
			String command = readCommand(commandFile);
			
			command = Utils.resolvePlaceholderIfExists(command, System.getProperties());
			System.out.println("Command To Run:"+command);
			in = new FileInputStream(nodesFile);
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new DataInputStream(in)));
			String strLine;

			String sshUser = System.getenv("USER");
			String userHome = System.getProperty("user.home");
			String sshIdentityLocation = userHome + FileSeparator + ".ssh"
					+ FileSeparator + "id_dsa";
			DuccRemoteLauncher remoteLauncher = new DuccRemoteLauncher(
					sshUser, sshIdentityLocation, System.out);
			//remoteLauncher.initialize();
			List<Future<ProcessCompletionResult>> results = new ArrayList<Future<ProcessCompletionResult>>();

			while ((strLine = br.readLine()) != null) {
				results.add(remoteLauncher.execute(strLine.trim(), command,
						this));
			}
			// Close the input stream
			in.close();
			for (Future<ProcessCompletionResult> result : results) {
				result.get();
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			if (in != null) {
				try {
					in.close();
				} catch (IOException ex) {
					// ignore
				}
			}
		}
	}

	/**
	 * Interprets and executes Admin command
	 * 
	 * @throws Exception
	 */
	public void process() throws Exception {
		if (commandLine.hasOption(DuccCommands.killAll.name())) {
			killAll();
		} else if (commandLine.hasOption(DuccCommands.startAgents.name())) {
			System.out.println("---------- Starting Agents");
			String[] args = commandLine
					.getOptionValues(DuccCommands.startAgents.name());
			startAgents(args[0], args[1]);
		} else if (commandLine.hasOption(DuccCommands.quiesceAgents.name())) {
      System.out.println("---------- Quiescing Agents");
      String[] args = commandLine
          .getOptionValues(DuccCommands.quiesceAgents.name());
      quiesceAgents(args[0]);
    }

	}

	public static void main(String[] args) {
		try {
			DuccAdmin admin = new DuccAdmin(new DefaultCamelContext());
			admin.parseWithPosixParser(args);
			admin.process();
		} catch (Throwable e) {
			e.printStackTrace();
		} finally {
			System.exit(-1);
		}
	}

	
	public void completed(ProcessCompletionResult result) {
		StringBuffer sb = new StringBuffer();
		if (result.exitCode != 0) {
			sb.append("DuccRemoteLauncher Failure. Host:").append(result.host)
					.append(" Error:").append(result.stderr)
					.append("\nCommand:").append(result.command);
		} else {
			sb.append("DuccRemoteLauncher Launched Command on Host:").append(
					result.host);
		}
		System.out.println(sb.toString());
	}

}
