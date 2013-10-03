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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.apache.uima.ducc.cli.IUiOptions.UiOption;
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

	private static String fixupEnvironment(String environment) {
	    // Rename the user's LD_LIBRARY_PATH as Secure Linuxs will not pass that on
	    boolean modified = false;
        String source = "LD_LIBRARY_PATH";
        String target = "DUCC_"+source;
        ArrayList<String> envList = tokenizeList(environment, false); // Don't strip quotes
        Map<String,String> envMap = parseAssignments(envList, false); // Keep all entries
        if (envMap.containsKey(source)) {
            if (!envMap.containsKey(target)) {
                envMap.put(target, envMap.get(source));
                envMap.remove(source);
                modified = true;
            }
        }
        // Augment user-specified environment with a few useful ones (only if not already set), e.g. USER HOME
        // If an augmented value contains a blank add single or double quotes 
        String envNames = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_submit_environment_propagated);
        if (envNames != null) {
            for (String name : envNames.split("\\s+")) {
                if (!envMap.containsKey(name)) {
                    String value = System.getenv(name);
                    if (value != null) {
                        if (value.indexOf(' ') >= 0) {
                            if (value.indexOf('"') < 0) {
                                value = "\"" + value + "\"";
                            } else if (value.indexOf('\'') < 0) {
                                value = "'" + value + "'";
                            } else {
                                System.out.println("WARNING: omitting environment variable " + name + " as has unquotable value: " + value);
                                continue;
                            }
                        }
                        envMap.put(name, value);
                        modified = true;
                    }
                }
            }
        }
        // If changes made rebuild the string ... note that quotes were preserved so can recreate easily
        if (modified) {
            StringBuilder sb = new StringBuilder();
            for (String name : envMap.keySet()) {
                sb.append(name).append("=").append(envMap.get(name)).append(" ");
            }
            return sb.toString();
        } else {
            return environment;
        }
	}
	
	public static void ducc_environment(CliBase base, Properties jobRequestProperties) {
	    String key = UiOption.Environment.pname();
		String environment_string = jobRequestProperties.getProperty(key, "");
		String fixedEnv = fixupEnvironment(environment_string);
        // If the same string returned, no need to change the property as it was not modified
        if (fixedEnv != environment_string) {
            jobRequestProperties.setProperty(key, fixedEnv);
        }
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
        NodeList nodes = doc.getElementsByTagName("inputQueue");
        if (nodes.getLength() > 0) {
            Element element = (Element) nodes.item(0);
            String endpoint = element.getAttribute("endpoint");
            String broker = element.getAttribute("brokerURL");
            String ep = "UIMA-AS:" + endpoint + ":" + broker;
            if (ep.contains("${")) {
                ArrayList<String> jvmargList = tokenizeList(jvmargs, true); // Strip quotes
                Map<String, String> jvmargMap = parseAssignments(jvmargList, true); // only -D entries
                ep = resolvePlaceholders(ep, jvmargMap);
            }
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
     * @param dependency_string This is the comma-delimited string of service ids "I" am dependent upon.
     */
    public static void check_service_dependencies(String endpoint, String dependency_string) 
    {
        if ( dependency_string == null ) {         // no dependencies to worry about
            return;
        }

        for (String d : dependency_string.split("\\s+")) {
            if (d.equals(endpoint)) {
                throw new IllegalArgumentException("A service cannot depend on itself: " + d);
            }
            String[] parts = d.split(":", 3);
            String type = parts[0];
            if (!type.equals(ServiceType.UimaAs.decode()) && !type.equals(ServiceType.Custom.decode())) {
                throw new IllegalArgumentException(
                                "Ill-formed or unsuported service type in dependency: " + d);
            }

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
                    throw new IllegalArgumentException("Invalid broker URL in service ID: " + broker);
                }
            }
        }
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
            matcher.appendReplacement(sb, value);        
        }
        matcher.appendTail(sb);
        return sb.toString();
    }
    
    /* 
     
     */
    
    /**
     * Create an array of parameters from a whitespace-delimited list (e.g. JVM args or environment assignments.) 
     * Values containing whitespace must be single- or double-quoted:
     *  TERM=xterm DISPLAY=:1.0 LD_LIBRARY_PATH="/my/path/with blanks/" EMPTY= -Dxyz="a b c" -Dabc='x y z' 
     * Quotes may be stripped or preserved.
     * Values containing both types of quotes are NOT supported.
     * 
     * @param options
     *          - string of blank-delimited options
     * @param stripQuotes
     *          - true if balanced quotes are to be removed
     * @return - array of options
     */
    public static ArrayList<String> tokenizeList(String options, boolean stripQuotes) {
        
      ArrayList<String> tokens = new ArrayList<String>();
      if (options == null) {
        return tokens;
      }
      
      // Pattern matches a non-quoted region or a double-quoted region or a single-quoted region
      // 1st part matches one or more non-whitespace characters but not " or '
      // 2nd part matches a "quoted" region containing any character except "
      // 3rd part matches a 'quoted' region containing any character except '
      // See: http://stackoverflow.com/questions/3366281/tokenizing-a-string-but-ignoring-delimiters-within-quotes
        
      String noSpaceRegex = "[^\\s\"']+";
      String doubleQuoteRegex = "\"([^\"]*)\"";
      String singleQuoteRegex = "'([^']*)'";
      final String regex = noSpaceRegex + "|" + doubleQuoteRegex + "|" + singleQuoteRegex;     
      Pattern patn = Pattern.compile(regex);
      Matcher matcher = patn.matcher(options);
      StringBuilder sb = new StringBuilder();
      
      // If stripping quotes extract the capturing group (without the quotes)
      // When preserving quotes extract the full region
      // Combine the pieces of a token until the match ends with whitespace
      if (stripQuotes) {
        while (matcher.find()) {
          if (matcher.group(1) != null) {
            sb.append(matcher.group(1));
          } else if (matcher.group(2) != null) {
            sb.append(matcher.group(2));
          } else {
            sb.append(matcher.group());
          }
          if (matcher.end() >= options.length() || Character.isWhitespace(options.charAt(matcher.end()))) {
            tokens.add(sb.toString());
            sb.setLength(0);
          }
        }
      } else {
        while (matcher.find()) {
          sb.append(matcher.group());
          if (matcher.end() >= options.length() || Character.isWhitespace(options.charAt(matcher.end()))) {
            tokens.add(sb.toString());
            sb.setLength(0);
          }
        }
      }
      return tokens;
    }

    /*
     * Create a map from an array of environment variable assignments produced by tokenizeList Quotes
     * may have been stripped by tokenizeList The value is optional but the key is NOT, e.g. accept
     * assignments of the form foo=abc & foo= & foo reject =foo & =
     * 
     * @param assignments - list of environment or JVM arg assignments
     * @param jvmArgs - true if tokens are JVM args -- process only the -Dprop=value entries
     *  
     * @return - map of key/value pairs null if syntax is illegal
     */
    public static Map<String, String> parseAssignments(List<String> assignments, boolean jvmArgs) 
        throws IllegalArgumentException {

      HashMap<String, String> map = new HashMap<String, String>();
      if (assignments == null || assignments.size() == 0) {
        return map;
      }
      for (String assignment : assignments) {
        String[] parts = assignment.split("=", 2); // Split on first '='
        String key = parts[0];
        if (key.length() == 0) {
          throw new IllegalArgumentException("Missing key in assignment: " + assignment);
        }
        if (jvmArgs) {
          if (!key.startsWith("-D")) {
            continue;
          }
          key = key.substring(2);
        }
        map.put(key, parts.length > 1 ? parts[1] : "");
      }
      return map;
    }

    // ====================================================================================================
    
    /*
     * Test the quote handling and optional stripping 
     */
    public static void main(String[] args) {
      String[] lists = { "SINGLE_QUOTED='single quoted'\tDOUBLE_QUOTED=\"double quoted\"     SINGLE_QUOTE=\"'\" \r DOUBLE_QUOTE='\"'",
                         "",
                         "            ",
                         null };
      
      for (String list : lists) { 
        System.out.println("List: " + list);
        ArrayList<String> tokens = tokenizeList(list, false);
        System.out.println("\n  quotes preserved on " + tokens.size());
        for (String token : tokens) {
          System.out.println("~" + token + "~");
        }
        tokens = tokenizeList(list, true);
        System.out.println("\n  quotes stripped from " + tokens.size());
        for (String token : tokens) {
          System.out.println("~" + token + "~");
        }
      }
    }
}
