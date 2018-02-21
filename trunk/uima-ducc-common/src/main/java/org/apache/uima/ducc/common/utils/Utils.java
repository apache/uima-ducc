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
package org.apache.uima.ducc.common.utils;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URL;
import java.rmi.server.UID;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.uima.ducc.common.IIdentity;
import org.springframework.util.PropertyPlaceholderHelper;


public class Utils {
  public static final String FileSeparator = System.getProperty("file.separator");
  
	public static boolean isIpAddress( String ip ) {
		String two_five_five = "(?:[0-9]|[1-9][0-9]|1[0-9][0-9]|2(?:[0-4][0-9]|5[0-5]))";
        Pattern IPPattern = Pattern.compile("^(?:"+two_five_five+"\\.){3}"+two_five_five+"$");
        return IPPattern.matcher(ip).matches();
	}
	public static int findFreePort() {
	    ServerSocket socket = null;
	    int port = 0;
	    try {
	      //  by passing 0 as an arg, let ServerSocket choose an arbitrary
	      //  port that is available.
	      socket = new ServerSocket(0);
	      port = socket.getLocalPort();
	    } catch (IOException e) {
	    } finally { 
	      try {
	        // Clean up
	        if (socket != null) {
	          socket.close(); 
	        } 
	      } catch( Exception ex) {
	    	  ex.printStackTrace();
	      }
	    }
	    return port;
	  }
	public static boolean portAvailable(int port ) {
		ServerSocket sock = null;
		try {
			sock = new ServerSocket();
			sock.bind(new InetSocketAddress(port));
			return true;
		} catch( Exception e) {
			return false;
		} finally {
		    if ( sock != null ) {
		    	try {
					sock.close();
		    	} catch( Exception e) {
		    		return false;
		    	}
		    }
		}
	}
	public static boolean isThisNode(String node, String thisNodeIP) throws Exception {
		if (Utils.isIpAddress(node)) {
			if (thisNodeIP.equals(node.trim()) ) {
				return true;
			} 
		}
		return false;
	}
	
	private static boolean isThisNode(String node, List<IIdentity> nodeIdentities) throws Exception {
		if (Utils.isIpAddress(node)) {
			for( IIdentity identity : nodeIdentities ) {
				if (identity.getIP().startsWith(node) ) {
					return true;
				} 
			}
		}
		return false;
	}
	
	
	
//	public static boolean isTargetNodeForMessage(String targetNodeList) throws Exception{
//		String[] nodes = targetNodeList.split(",");
//		for ( String node : nodes ) {
//			if ( isThisNode(node) ) {
//				return true;
//			}
//		}
//		return false;
//	}
	public static boolean isTargetNodeForMessage(String targetNodeList, String thisNodeIP) throws Exception{
		String[] nodes = targetNodeList.split(",");
		for ( String node : nodes ) {
			if( isThisNode(node,thisNodeIP) ) {
				return true;
			}
		}
		return false;
	}
	public static boolean isTargetNodeForMessage(String targetNodeList, List<IIdentity> nodeIdentities) throws Exception{
		String[] nodes = targetNodeList.split(",");
		for ( String node : nodes ) {
			if( isThisNode(node,nodeIdentities) ) {
				return true;
			}
		}
		return false;
	}
	public static Properties loadPropertiesFromClasspathForResource(String resource) throws Exception {
		InputStream in = null;
		Properties properties = new Properties();
		ClassLoader loader = Thread.currentThread ().getContextClassLoader ();
		if ( !resource.endsWith(".properties") ) {
			resource += ".properties";
		}
		in = loader.getResourceAsStream (resource);
        if (in != null)
        {
        	properties = new Properties ();
        	properties.load (in); // Can throw IOException
        } else {
        	throw new IOException("Process Group Configuration File:"+resource+".properties Not Found in the Classpath");
        }
		return properties;
	}
	public static List<String> getHostsFromFile(String hostFilePath)
			throws Exception {
		List<String> nodeList = new ArrayList<String>();
		File nodesFile = new File(hostFilePath);
		if (nodesFile.exists()) {
			// Open the file that is the first
			// command line parameter
			FileInputStream fstream = new FileInputStream(nodesFile);
			DataInputStream in = null;
			try {
				// Get the object of DataInputStream
				in = new DataInputStream(fstream);
				BufferedReader br = new BufferedReader(
						new InputStreamReader(in));
				String node;
				// Read File Line By Line
				while ((node = br.readLine()) != null) {
					// Print the content on the console
					nodeList.add(node);
				}
			} catch (Exception e) {
				throw e;
			} finally {
				// Close the input stream
				if(in != null) {
					in.close();
				}
			}
		}
		return nodeList; // empty list
	}
	public static String generateUniqueId() {
		return new UID().toString();
	}

	public static boolean isLinux() {
		return System.getProperty("os.name").toLowerCase().equals("linux");
	}
	public static boolean isWindows() {
		return System.getProperty("os.name").toLowerCase().startsWith("windows");
	}
	public static boolean isMac() {
		return System.getProperty("os.name").toLowerCase().startsWith("mac");
	}
	public static String getPID() {
		String pid = ManagementFactory.getRuntimeMXBean().getName();
		return pid.split("@")[0];
	}
	public static boolean isNumber(String number) {
		try {
			Integer.parseInt(number);
			return true;
		} catch( NumberFormatException e) {
			return false;
		}
	}
  /**
   * Resolves placeholders in provided contents using java's Matcher. Finds
   * all occurances of ${<placeholder>} and resolves each using System properties
   * which holds <placeholder>=<value> pairs.
   *  
   * @param contents - target text containing placeholder(s)
   * @param props - Properties object holding key/value pairs
   * @return - text with resolved placeholders
   * 
   * @throws Exception
   */
    public static String resolvePlaceholders(String contents) 
    {
        return resolvePlaceholders(contents, System.getProperties());
    }

	/**
	 * Resolves placeholders in provided contents using java's Matcher. Finds
	 * all occurances of ${<placeholder>} and resolves each using provided
	 * Properties object which holds <placeholder>=<value> pairs.
	 * If the placeholder not found then tries the System properties.
	 *  
	 * @param contents - target text containing placeholder(s)
	 * @param props - Properties object holding key/value pairs
	 * @return - text with resolved placeholders
	 * 
	 * @throws Exception
	 */
	public static String resolvePlaceholders(String contents, Properties props ) 
    {
        //  Placeholders syntax ${<placeholder>}
        Pattern placeHolderPattern = Pattern.compile("\\$\\{(.*?)\\}");
      
        java.util.regex.Matcher matcher = 
            placeHolderPattern.matcher(contents); 

        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            // extract placeholder
            final String key = matcher.group(1);
            //  Find value for extracted placeholder. 
            String placeholderValue = props.getProperty(key);
            if (placeholderValue == null) {
                placeholderValue = System.getProperty(key);
                if (placeholderValue == null) {
                    throw new IllegalArgumentException("Missing value for placeholder: " + key);
                }
            }
            matcher.appendReplacement(sb, placeholderValue);        
        }
        matcher.appendTail(sb);
        return sb.toString();
	}
	
	/**
	 * Resolves placeholder using Spring Framework utility class
	 *  
	 * 
	 * @param value
	 * @param props
	 * @return
	 */
	public static String resolvePlaceholderIfExists(String value, Properties props ) {
		findDuccHome();  // add DUCC_HOME to System.properties

		if ( value != null && value.contains("${")) {
            PropertyPlaceholderHelper pph = new PropertyPlaceholderHelper("${","}");
            value = pph.replacePlaceholders(value, props);
        }
		return value;  
	}
	/**
	 * Concatenates multiple arrays into one array of type <A> 
	 * 
	 * @return array of type <A>
	 */
	public static <A> A[] concatAllArrays(A[] first, A[]... next) {
		int totalLength = first.length;
		//	compute the total size of all arrays
		for (A[] array : next) {
			totalLength += array.length;
		}
		A[] result = Arrays.copyOf(first, totalLength);
		int offset = first.length;
		for (A[] array : next) {
			System.arraycopy(array, 0, result, offset, array.length);
			offset += array.length;
		}
		return result;
	}
	public static int getPID(Process process) {
		int pid = -1;
		if (process.getClass().getName().equals("java.lang.UNIXProcess")) {
			try {
				Field f = process.getClass().getDeclaredField("pid");
				f.setAccessible(true);
				pid = f.getInt(process);
			} catch (Throwable e) {
				// ignore
			}
		}
		return pid;
	}
	
	private static boolean compare(String s1, String s2) {
		boolean retVal = false;
		if(s1 != null) {
			if(s2 != null) {
				if(s1.equals(s2)) {
					retVal = true;
				}
			}
		}
		return retVal;
	}
	
	public static boolean isMachineNameMatch(String m1, String m2) {
		boolean retVal = false;
		if(compare(m1,m2)) {
			retVal = true;
		}
		else {
			int ndx1 = m1.indexOf(".");
		    int ndx2 = m2.indexOf(".");
		    if ( (ndx1 > 0) && (ndx2 > 0) ) {
		       	//retVal = false;  
		    }
		    else {
		      	String n1 = m1;
		      	if ( ndx1 > 0 ) {
		      		n1 = m1.substring(0, ndx1);
		       	}
		       	String n2 = m2;
		       	if ( ndx2 > 0 ) {
		       		n2 = m2.substring(0, ndx2);
		       	}
		       	if(compare(n1,n2)) {
					retVal = true;
				}
			}
		}
		return retVal;
	}

	/*
	 * Return the value of DUCC_HOME 
	 * Infer DUCC_HOME from location of this class
	 * but let system property override, and warn when different.
	 */
	
	static String DUCC_HOME = null;
	
	public static String findDuccHome() {

		if (DUCC_HOME != null) {
			return DUCC_HOME;
		}
		DUCC_HOME = System.getProperty("DUCC_HOME");

		// Find resource that holds this class and if a jar check if it appears to be in a DUCC installation
		URL res = Utils.class.getProtectionDomain().getCodeSource().getLocation();
		String p = res.getPath();
		if (!p.endsWith(".jar")) {
			if (DUCC_HOME == null) {
				throw new IllegalArgumentException(
						"DUCC_HOME system property missing and cannot infer it as not running from a jar");
			}
			return DUCC_HOME;
		}
		// File name should be:
		// <ducc-home>/lib/uima-ducc/uima-ducc-common-<version>.jar
		// Strip off the jar file and the 2 directories above
		int ndx = p.lastIndexOf("/");
		ndx = p.lastIndexOf("/", ndx - 1);
		ndx = p.lastIndexOf("/", ndx - 1);
		String jar_ducc_home = p.substring(0, ndx);
		File props = new File(jar_ducc_home + "/resources/ducc.properties");
		if (!props.exists()) {
			if (DUCC_HOME == null) {
				throw new IllegalArgumentException("DUCC_HOME system property missing and cannot infer it as "
						+ res + " is not part of a valid DUCC installation");
			}
			return DUCC_HOME;
		}

		if (DUCC_HOME == null) {
			DUCC_HOME = jar_ducc_home;
			System.setProperty("DUCC_HOME", DUCC_HOME);
			
		} else { // Resolve any symbolic links, e.g. /users -> /users1
			try {
				DUCC_HOME = (new File((DUCC_HOME))).getCanonicalPath();
			} catch (IOException e) {
			}
			if (!jar_ducc_home.equals(DUCC_HOME)) {
				System.out.println("WARNING: Setting DUCC_HOME = " + DUCC_HOME
						+ " but the CLI request is from " + jar_ducc_home);
			}
		}

		return DUCC_HOME;
	}
	public static int getMaxSystemUserId() {
		String tmp;
		int uidMax=500;
		
	    if ((tmp = System
				.getProperty("ducc.agent.rogue.process.sys.uid.max")) != null) {
			if ( isNumber(tmp) ) {
				uidMax = Integer.valueOf(tmp);
			}
		} 
	    return uidMax;
	}
	public static void main(String[] args) {
		try {
			if ( Utils.isThisNode("192.168.3.3", "192.168.3.3") ) {
				System.out.println("Nodes equal");
			} else {
				System.out.println("Nodes NOT equal");
			}
		} catch( Exception e) {
			e.printStackTrace();
		}
	}
}
