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

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.uima.ducc.common.IDuccUser;
import org.apache.uima.ducc.common.TcpStreamHandler;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.QuotedOptions;
import org.apache.uima.ducc.transport.event.sm.IService.ServiceType;
import org.apache.uima.ducc.user.common.UimaUtils;
import org.apache.uima.util.XMLInputSource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;


public class DuccUiUtilities {
	
	public static String getUser() {
		String user = System.getProperty("user.name");
		String runmode = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_runmode);
		if(runmode != null) {
			if(runmode.equals("Test")) {
				String envUser = System.getenv(IDuccUser.EnvironmentVariable.USER.value());
				if(envUser != null) {
					user = envUser;
				}
			}
		}
		return user;
	}

  public static String fixupEnvironment(String environment, String allInOne) {
	  
    ArrayList<String> envList = QuotedOptions.tokenizeList(environment, false); // Don't strip quotes
    Map<String, String> envMap = QuotedOptions.parseAssignments(envList, +1); // Expand any FOO or FOO* entries
    
    // Rename the user's LD_LIBRARY_PATH as Secure Linuxs will not pass that on
    // But not for --all-in-one local
    if (allInOne == null || !allInOne.equalsIgnoreCase("local")) {
      String source = "LD_LIBRARY_PATH";
      String target = "DUCC_" + source;
      if (envMap.containsKey(source)) {
        if (!envMap.containsKey(target)) {
          envMap.put(target, envMap.get(source));
          envMap.remove(source);
        }
      }
    }
    
    // Augment user-specified environment with a few useful ones (only if not already set), e.g. USER HOME
    String envNames = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_environment_propagated);
    if (envNames != null) {
      for (String name : envNames.split("\\s+")) {
        if (!envMap.containsKey(name)) {
          String value = QuotedOptions.quoteValue(name);  // Quote value if necessary
          if (value != null) {
            envMap.put(name, value);
          }
        }
      }
    }
    // Must rebuild the string ... note that quotes were preserved so can recreate easily
    StringBuilder sb = new StringBuilder();
    for (String name : envMap.keySet()) {
      sb.append(name).append("=").append(envMap.get(name)).append(" ");
    }
    return sb.toString();
  }
	
	/* 
	 * Get URL for service handling request. Either "orchestrator" or "sm"
	 */
	public static String dispatchUrl(String server) {
	    String host = DuccPropertiesResolver.get("ducc." + server + ".http.node");
	    String port = DuccPropertiesResolver.get("ducc." + server + ".http.port");
        if ( host == null || port == null) {
            throw new IllegalStateException("ducc." + server + ".http.node and/or .port not set in ducc.properties");
        }
        return "http://" + host + ":" + port + "/" + server.substring(0, 2);
	}
	
    /**
     * Extract the endpoint from the deployment descriptor, resolving names and placeholders against
     * the same environment as that of the JVM that will deploy the service 
     * 
     * @param working_dir
     * @param process_DD
     * @param jvmargs
     * @return
     */
    public static String getEndpoint(String working_dir, String process_DD, String jvmargs) {
        // convert relative path for process_DD to absolute if needed
        if (!process_DD.startsWith("/") && process_DD.endsWith(".xml") && working_dir != null) {
            process_DD = working_dir + "/" + process_DD;
        }

        // parse process_DD into DOM, resolving the descriptor either by name or by location
        Document doc = null;
        try {
            XMLInputSource xmlin = UimaUtils.getXMLInputSource(process_DD);
            DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            doc = db.parse(xmlin.getInputStream());
        } catch (Throwable t) {
            t.printStackTrace();
            throw new IllegalArgumentException(t.getMessage());
        }

        // locate the <inputQueue node within the xml - should only be one such node, and it MUST exist
        // then construct an endpoint and resolve any placeholders against the process JVM args
        // just as is done by Spring in a UIMA-AS Deployment Descriptor
        // Ignore any decorations on the broker URL as they are not part of the service name
        NodeList nodes = doc.getElementsByTagName("inputQueue");
        if (nodes.getLength() > 0) {
            Element element = (Element) nodes.item(0);
            String endpoint = element.getAttribute("endpoint");
            String broker = element.getAttribute("brokerURL");
            if (endpoint.contains("${") || broker.contains("${")) {
                ArrayList<String> jvmargList = QuotedOptions.tokenizeList(jvmargs, true); // Strip quotes
                Map<String, String> jvmargMap = QuotedOptions.parseAssignments(jvmargList, -1); // only -D entries
                endpoint = resolvePlaceholders(endpoint, jvmargMap);
                broker = resolvePlaceholders(broker, jvmargMap);
            }
            int i = broker.indexOf('?');
            if (i > 0) {
            	broker = broker.substring(0, i);
            }
            String ep = "UIMA-AS:" + endpoint + ":" + broker;
            return ep;
        } else {
            throw new IllegalArgumentException("Invalid DD:" + process_DD + ". Missing required element <inputQueue ...");
        }
    }

    /**
     * Check that dependencies are syntactically correct, and that a service doesn't depend on itself.
     *
     * Assumes that any placeholders have been resolved against the caller's environment
     *
     * @param endpoint This is the endpoint of the caller itself, for resolution ( to make sure it can resolve.).  For
     *                 jobs this must be null.
     * @param dependency_string This is the whitespace-delimited string of service ids "I" am dependent upon.
     * 
     * @return (possibly) corrected list of dependencies
     */
    public static String check_service_dependencies(String endpoint, String dependency_string) 
    {
        if ( dependency_string == null ) {         // no dependencies to worry about
            return null;
        }

        StringBuilder deps = new StringBuilder();
        for (String d : dependency_string.split("\\s+")) {
            String[] parts = d.split(":", 3);
            String type = parts[0];

            if (type.equals(ServiceType.UimaAs.decode())) {
                // MUST have 2 ":" in it, and the broker must be a valid url
                // UIMA-AS:queuename:broker
                if (parts.length < 3) {
                    throw new IllegalArgumentException("Invalid UIMA-AS service id: " + d);
                }
                String qname = parts[1];
                String broker = parts[2];
                if (qname.equals("") || broker.equals("")) {
                    throw new IllegalArgumentException("Invalid syntax for UIMA-AS service id: " + d);
                }
                // this IS unused, it is here only to insure the string is parsed as a URL
                @SuppressWarnings("unused")
                URL url = null;
                try {
                    url = new URL(null, broker, new TcpStreamHandler());
                } catch (MalformedURLException e) {
                    throw new IllegalArgumentException("Invalid broker URL '" + broker + "' in service ID '" + d + "'");
                }
                // Finally strip the decorations as they are not part of a service name
                // (Could check with url.getQuery() but cannot easily rebuild without it)
                int ix = broker.indexOf('?');
                if ( ix > 0) {
                    System.out.println("WARNING: Ignoring URL decorations on service ID " + d);
                    d = parts[0] + ":" + parts[1] + ":" + broker.substring(0, ix); 
                }
            }  else if (!type.equals(ServiceType.Custom.decode())) {
                throw new IllegalArgumentException(
                        "Ill-formed or unsupported service type in dependency: '" + d + "'");
            }
            
            if (d.equals(endpoint)) {
                throw new IllegalArgumentException("A service cannot depend on itself: " + d);
            }
            deps.append(d).append(" ");
        }
        return deps.substring(0, deps.length()-1);
    }

    /*
     * Resolve any ${..} placeholders against a map of JVM arg values
     */
    private static String resolvePlaceholders(String contents, Map<String,String> argMap) {
        //  Placeholders syntax ${<placeholder>} 
        Pattern pattern = Pattern.compile("\\$\\{(.*?)\\}");  // Stops on first '}'
        Matcher matcher = pattern.matcher(contents); 

        StringBuffer sb = new StringBuffer();
        while (matcher.find()) {
            final String key = matcher.group(1);
            String value = argMap.get(key);
            if (value == null) {
                throw new IllegalArgumentException("Undefined JVM property '" + key + "' in: " + contents);
            }
            // value may contain unreplaced ${..} sequence which looks like a group reference so replace as-is
            matcher.appendReplacement(sb, "");
            sb.append(value);
        }
        matcher.appendTail(sb);
        return sb.toString();
    }
    
}
