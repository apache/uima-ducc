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

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.uima.ducc.common.TcpStreamHandler;
import org.apache.uima.ducc.common.uima.UimaUtils;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.transport.event.sm.IService.ServiceType;
import org.apache.uima.util.XMLInputSource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;


public class DuccUiUtilities {
	
	public static boolean isSupportedBeta() {
		boolean retVal = true;
		String key = DuccPropertiesResolver.ducc_submit_beta;
		String value = DuccPropertiesResolver.get(key);
		if(value != null) {
			if(value.equalsIgnoreCase("off")) {
				retVal = false;
			}
		}
		return retVal;
	}
	
	public static String getUser() {
		String user = System.getProperty("user.name");
		String runmode = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_runmode);
		if(runmode != null) {
			if(runmode.equals("Test")) {
				String envUser = System.getenv("USER");
				if(envUser != null) {
					user = envUser;
				}
			}
		}
		return user;
	}
	
	/*
	 * Create a map from the user-specified environment string
	 * Must be a white-space delimited string of assignments, e.g. 
	 *     TERM=xterm DISPLAY=:1.0 LD_LIBRARY_PATH=/my/own/path
	 * Keys & values cannot contain white-space, e.g. all of these will fail:
	 *     TERM= xterm DISPLAY =:1.0 DUCC_LD_LIBRARY_PATH="/my/o n/path"  
	 */
	private static Properties environmentMap(String environment) {
		Properties properties = new Properties();
		if(environment != null) {
			String[] tokens = environment.split("\\s+");
			for( String token : tokens) {
				String[] nvp = token.split("=");
				if(nvp.length > 1) {
					String key = nvp[0];
					String value = nvp[1];
					properties.put(key, value);
				} else {
				    return null;
				}
			}
		}
		// No need to trim properties as the only white-space is between assignments
		return properties;
	}
	
	public static ArrayList<String> getDuplicateOptions(CommandLine commandLine) {
		ArrayList<String> duplicates = new ArrayList<String>();
		HashMap<String,String> seen = new HashMap<String,String>();
		Option[] options = commandLine.getOptions();
		for(Option option : options) {
			String name = option.getLongOpt();
			if(seen.containsKey(name)) {
				if(!duplicates.contains(name)) {
					duplicates.add(name);
				}
			}
			else {
				seen.put(name,name);
			}
		}
		return duplicates;
	}
	
	//**********
	
// 	public static boolean duplicate_options(IDuccMessageProcessor duccMessageProcessor, CommandLine commandLine) {
// 		boolean retVal = false;
// 		ArrayList<String> duplicates = DuccUiUtilities.getDuplicateOptions(commandLine);
// 		if(!duplicates.isEmpty()) {
// 			for(String duplicate : duplicates) {
// 				duccMessageProcessor.err("duplicate option: "+duplicate);
// 			}
// 			retVal = true;
// 		}
// 		return retVal;
// 	}
	
	//**********
	
	public static boolean ducc_environment(CliBase base, Properties jobRequestProperties, String key) {
		boolean retVal = true;
		// Rename the user's LD_LIBRARY_PATH as Secure Linuxs will not pass that on
		String source = "LD_LIBRARY_PATH";
		String target = "DUCC_"+source;
		String environment_string = jobRequestProperties.getProperty(key);
		Properties environment_properties = environmentMap(environment_string);
		if (environment_properties == null) {
		    base.message("ERROR:", key, "Invalid environment syntax - missing '=' ?");
		    return false;
		}
		if(environment_properties.containsKey(source)) {
			if(environment_properties.containsKey(target)) {
				base.message("ERROR:", key, "environment conflict:", target, "takes precedence over", source);
			}
			else {
				target += "="+environment_properties.getProperty(source);
				environment_string += " "+target;
				jobRequestProperties.setProperty(key, environment_string);
				//duccMessageProcessor.out(key+": "+environment_string);
			}
		}
		// Augment user-specified environment with a few useful ones, e.g. USER HOME
        String envNames = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_submit_environment_propagated);
        if (envNames != null) {
            StringBuilder sb = new StringBuilder();
            for (String name : envNames.split("\\s+")) {
                if (!environment_properties.containsKey(name)) {
                    sb.append(name).append("=").append(System.getenv(name)).append(" ");
                }
            }
            if (environment_string != null) {
                sb.append(environment_string);
            }
            jobRequestProperties.setProperty(key, sb.toString());
        }
		return retVal;
	}
	
	//**********
	
	public static String buildBrokerUrl() {
		String ducc_broker_protocol = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_broker_protocol);
		String ducc_broker_hostname = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_broker_hostname);
		String ducc_broker_port = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_broker_port);
		String ducc_broker_url_decoration = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_broker_url_decoration);
		if(ducc_broker_protocol == null) {
			ducc_broker_protocol = "tcp";
		}
		if(ducc_broker_hostname == null) {
			ducc_broker_hostname = "localhost";
		}
		if(ducc_broker_port == null) {
			ducc_broker_port = "61616";
		}
		StringBuffer sb = new StringBuffer();
	    sb.append(ducc_broker_protocol);
	    sb.append("://");
	    sb.append(ducc_broker_hostname);
	    sb.append(":");
	    sb.append(ducc_broker_port);
	    if( ducc_broker_url_decoration != null ) {
	    	ducc_broker_url_decoration = ducc_broker_url_decoration.trim();
	    	if( ducc_broker_url_decoration.length() > 0 ) {
	    		sb.append("?");
	    		sb.append(ducc_broker_url_decoration);
	    	}
	    }
	    return sb.toString();
	}

    /**
     * Resolve all properies in a props from jvmargs and System.properties().
     */
    public static void  resolvePropertiesPlaceholders(Properties myprops, Properties jvmargs)
    {

        Properties sysprops = System.getProperties();
        for ( Object o : myprops.keySet() ) {
            //
            // We're resolving against serveral properties files.  We have to catch and ignore
            // missing-placeholder exceptions until the last one, because missing stuff could be
            // resolved in subsequent attampts.
            //
            String k = (String) o;
            Object vo = myprops.get(k);            
            if ( !( vo instanceof String) ) {       // some things aren't strings, we bypass them
                continue;
            }

            String v = (String) myprops.get(k);
            try {
                v = Utils.resolvePlaceholders(v, sysprops);
            } catch ( IllegalArgumentException e ) {
                // ignore this one if it occurs, not the next one
            }
            if ( jvmargs != null ) {
                v = Utils.resolvePlaceholders(v, jvmargs);
            }
            myprops.put(k, v);
        }        
    }

    /**
     * Given a jvmargs-like string, turn it into a properties object containing the -D entries
     * and resolve against system properites.
     *
     * Try to only call this once, it's a boot-strap for getting the JVM args.
     */
    public static Properties jvmArgsToProperties(String jvmargs)
    {
        if ( jvmargs == null ) return null;

        // We have to make a properties file from the jvm args.  Resolve placeholders
        // and return the props file with everything resolved.
        Properties props = new Properties();

        //
        // The jvm args look like properties for the most part: k=v pairs
        // We write them into a string and turn them into properies for the resolver
        //
        String[] args = jvmargs.split("\\s+");
        StringWriter sw = new StringWriter();
        for ( String s : args ) sw.write(s + "\n");
        StringReader sr = new StringReader(sw.toString());            
        try {
            props.load(sr);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        //
        // Not quite there -- only -D things are props, and they have '-D' stuck to their names!
        //
        Properties goodprops = new Properties();
        Properties sysprops = System.getProperties();
        for ( Object s : props.keySet() ) {
            String k = (String) s;
            if ( k.startsWith("-D") ) {
                String v = (String) props.get(k);
                k = k.substring(2);
                k = Utils.resolvePlaceholders(k, sysprops);
                goodprops.put(k, v);
            }
        }

        return goodprops;
    }

    public static String getEndpoint(String working_dir, String process_DD, Properties jvmargs)
    {

    	// convert relative path for process_DD to absolute if needed
        if ( !process_DD.startsWith("/") && process_DD.endsWith(".xml") && working_dir != null ) {
            process_DD = working_dir + "/" + process_DD;
        }

        //  parse process_DD into DOM, resolving the descriptor either by name or by location
        Document doc = null;
		try {
			XMLInputSource xmlin = UimaUtils.getXMLInputSource(process_DD);
			DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
			doc = db.parse(xmlin.getInputStream());
		} catch ( Throwable t ) {
            t.printStackTrace();
            throw new IllegalArgumentException(t.getMessage());
		}
                
        // locate the <inputQueue node within the xml - should only be one such node, and it MUST exist
        // then construct an endpoint and resolve defaultBrokerURL if needed
        NodeList nodes = doc.getElementsByTagName("inputQueue");
        if ( nodes.getLength() > 0 ) {
            Element element = (Element) nodes.item(0);
            String endpoint = element.getAttribute("endpoint");
            String broker   = element.getAttribute("brokerURL");
            String ep = "UIMA-AS:" + endpoint + ":" + broker;

            ep = Utils.resolvePlaceholders(ep, jvmargs);
            ep = Utils.resolvePlaceholders(ep, System.getProperties());
            return ep;
        } else {
            throw new IllegalArgumentException("Invalid DD:" + process_DD + ". Missing required element <inputQueue ...");
        }
    }

    /**
     * Must resolve ${defaultBrokerURL} from -DdefaultBrokerURL and if it fails, fail the job because
     * the defaut of "tcp://localhost:61616" is never correct for a DUCC job.
     *
     * We also split up the string and examine each endpoint so we can fail if invalid endpoints are given.
     *
     * And, we do a quick check for circular dependencies.  At this point all we can check for is no duplicates
     * in the set[endpoint, dependencies].
     *
     * @param endpoint This is the endpoint of the caller itself, for resolution ( to make sure it can resolve.).  For
     *                 jobs this must be null.
     * @parem dependency_string This is the comma-delimeted string of service ids "I" am dependent upon.
     * @param jvmargs These are the JVM arguments specified for the job, converted to a properties file.
     */
    public static String resolve_service_dependencies(String endpoint, String dependency_string, Properties jvmargs) 
    {

        if ( dependency_string == null ) {         // no dependencies to worry about
            return null;
        }

        String[] deplist = dependency_string.split("\\s");
        Map<String, String> resolved = new HashMap<String, String>();
        if ( endpoint != null ) {
            resolved.put(endpoint, endpoint);
        }
        int ndx = 0;

        for ( String d : deplist ) {
            d = d.trim();
            if ( d.startsWith(ServiceType.UimaAs.decode() + ":") || d.startsWith(ServiceType.Custom.decode() + ":") ) {
                String nextdep = Utils.resolvePlaceholders(d, jvmargs);                
                if ( resolved.containsKey(nextdep) ) {
                    throw new IllegalArgumentException("Circular dependencies with " + nextdep);
                }
        
                if ( d.startsWith(ServiceType.UimaAs.decode()) ) {
                    // hard to know if this is a good EP or not but se do know that it MUST have 2 ":" in it, and the broker must be a valid url
                    // UIMA-AS : queuename : broker
                    // This code comes from SM:ServiceSet.parseEndpoint.  How best to generalize these?
                    ndx = d.indexOf(":");
                    if ( ndx <= 0 ) {
                        throw new IllegalArgumentException("Invalid UIMA-AS service id: " + d);                        
                    }

                    d = d.substring(ndx+1);
                    ndx = d.indexOf(":");
                    if ( ndx <= 0 ) {
                        throw new IllegalArgumentException("Invalid UIMA-AS service id (missing or invalid broker URL): " + d);
                    }
                    String qname    = d.substring(0, ndx).trim();
                    String broker   = d.substring(ndx+1).trim();
                    
                    @SuppressWarnings("unused")
                    // this IS unused, it is here only to insure the string is parsed as a URL
					URL url = null;
                    try {                
                        url = new URL(null, broker, new TcpStreamHandler());
                    } catch (MalformedURLException e) {
                        throw new IllegalArgumentException("Invalid broker URL in service ID: " + broker);
                    }
                    
                    if ( qname.equals("") || broker.equals("") ) {
                        throw new IllegalArgumentException("The endpoint cannot be parsed.  Expecting UIMA-AS:Endpoint:Broker, received " + d);
                    }
                    
                }
                                            
                resolved.put(nextdep, nextdep);
            } else {
                throw new IllegalArgumentException("Ill-formed or unsuported service type in dependency: " + d);
            }
        }
        
        if ( endpoint != null ) {
            resolved.remove(endpoint);                   // remember to remove "me"!
        }
        StringBuffer sb = new StringBuffer();
        ndx = 0;
        int len = resolved.size();
        for ( String s : resolved.keySet() ) {
            sb.append(s);
            if ( (++ndx ) < len ) {
                sb.append(" ");
            }
        }
        return sb.toString();
    }

}
