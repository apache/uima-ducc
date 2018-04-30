package org.apache.uima.ducc.ps.service.jmx;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.rmi.registry.LocateRegistry;

import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.uima.ducc.ps.service.errors.ServiceInitializationException;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class JMXAgent {
	private JMXConnectorServer jmxConnector;
	private Logger logger;
	private String assignedJmxPort;
	
	public JMXAgent(String assignedJmxPort, Logger logger) {
		this.assignedJmxPort = assignedJmxPort;
		this.logger = logger;
	}
	
	public int initialize() throws ServiceInitializationException {
		String key = "com.sun.management.jmxremote.authenticate";
		String value = System.getProperty(key);
		logger.log(Level.INFO, key + "=" + value);
		int rmiRegistryPort = 2099; // start with a default port setting
		if (assignedJmxPort != null) {
			try {
				int tmp = Integer.parseInt(assignedJmxPort);
				rmiRegistryPort = tmp;
			} catch (NumberFormatException nfe) {
				// default to 2099
				logger.log(Level.WARNING, "startJmxAgent", nfe);
			}
		}
		boolean done = false;
//		JMXServiceURL url = null;
		// retry until a valid rmi port is found
		while (!done) {
			try {
				LocateRegistry.createRegistry(rmiRegistryPort);
				done = true;
				// Got a valid port
			} catch (Exception exx) {
				// Try again with a different port
				rmiRegistryPort++;
			}
		} // while
		return rmiRegistryPort;
	}
	/**
	 * Start RMI registry so the JMX clients can connect to the JVM via JMX.
	 * 
	 * @return JMX connect URL
	 * @throws Exception
	 */
	public String start(int rmiRegistryPort) throws ServiceInitializationException {
		JMXServiceURL url = null;
		try {
			MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

			String hostname = InetAddress.getLocalHost().getHostName();

			String s = String.format("service:jmx:rmi:///jndi/rmi://%s:%d/jmxrmi", hostname, rmiRegistryPort);
			url = new JMXServiceURL(s);
			jmxConnector = JMXConnectorServerFactory.newJMXConnectorServer(url, null, mbs);
			jmxConnector.start();
		} catch (Exception e) {
			url = null;
			logger.log(Level.WARNING,
					"startJmxAgent Unable to Start JMX Connector. Running with *No* JMX Connectivity");
		}
		if (url == null) {
			return ""; // empty string
		} else {
			return url.toString();
		}
	}
	public void stop() throws IOException {
		jmxConnector.stop();
	}
}
