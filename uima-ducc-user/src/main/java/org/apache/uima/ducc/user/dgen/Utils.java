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
package org.apache.uima.ducc.user.dgen;

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
import java.rmi.server.UID;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

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
	    try {
	      //  by passing 0 as an arg, let ServerSocket choose an arbitrary
	      //  port that is available.
	      socket = new ServerSocket(0);
	    } catch (IOException e) {
	    } finally { 
	      try {
	        // Clean up
	        if (socket != null) {
	          socket.close(); 
	        } 
	      } catch( Exception ex) {}
	    }
	    return socket.getLocalPort();
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
				in.close();
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
