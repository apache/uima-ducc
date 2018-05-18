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
package org.apache.uima.ducc.ps.service.utils;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {
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

	/**
	 * Resolves placeholders in provided contents using java's Matcher. Finds
	 * all occurances of ${<placeholder>} and resolves each using System
	 * properties which holds <placeholder>=<value> pairs.
	 * 
	 * @param contents
	 *            - target text containing placeholder(s)
	 * @param props
	 *            - Properties object holding key/value pairs
	 * @return - text with resolved placeholders
	 * 
	 * @throws Exception
	 */
	public static String resolvePlaceholders(String contents) {
		return resolvePlaceholders(contents, System.getProperties());
	}

	/**
	 * Resolves placeholders in provided contents using java's Matcher. Finds
	 * all occurances of ${<placeholder>} and resolves each using provided
	 * Properties object which holds <placeholder>=<value> pairs. If the
	 * placeholder not found then tries the System properties.
	 * 
	 * @param contents
	 *            - target text containing placeholder(s)
	 * @param props
	 *            - Properties object holding key/value pairs
	 * @return - text with resolved placeholders
	 * 
	 * @throws Exception
	 */
	public static String resolvePlaceholders(String contents, Properties props) {
		// Placeholders syntax ${<placeholder>}
		Pattern placeHolderPattern = Pattern.compile("\\$\\{(.*?)\\}");

		java.util.regex.Matcher matcher = placeHolderPattern.matcher(contents);

		StringBuffer sb = new StringBuffer();
		while (matcher.find()) {
			// extract placeholder
			final String key = matcher.group(1);
			// Find value for extracted placeholder.
			String placeholderValue = props.getProperty(key);
			if (placeholderValue == null) {
				placeholderValue = System.getProperty(key);
				if (placeholderValue == null) {
					throw new IllegalArgumentException(
							"Missing value for placeholder: " + key);
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
	public static String resolvePlaceholderIfExists(String value,
			Properties props) {
		String retVal = value;
		if (value != null && value.contains("${")) {
			retVal = resolvePlaceholders(value, props);
		}
		return retVal;
	}
	public static URLClassLoader create(String classPath)
			throws MalformedURLException {
		return create(classPath.split(":"));
	}

	public static URLClassLoader create(String[] classPathElements)
			throws MalformedURLException {
		ArrayList<URL> urlList = new ArrayList<URL>(classPathElements.length);
		for (String element : classPathElements) {
			if (element.endsWith("*")) {
				File dir = new File(element.substring(0, element.length() - 1));
				File[] files = dir.listFiles(); // Will be null if missing or
												// not a dir
				if (files != null) {
					for (File f : files) {
						if (f.getName().endsWith(".jar")) {
							urlList.add(f.toURI().toURL());
						}
					}
				}
			} else {
				File f = new File(element);
				if (f.exists()) {
					urlList.add(f.toURI().toURL());
				}
			}
		}
		URL[] urls = new URL[urlList.size()];
		return new URLClassLoader(urlList.toArray(urls), ClassLoader
				.getSystemClassLoader().getParent());
	}

	/*
	 * Dump all the URLs
	 */
	public static void dump(ClassLoader cl, int numLevels) {
		int n = 0;
		for (URLClassLoader ucl = (URLClassLoader) cl; ucl != null
				&& ++n <= numLevels; ucl = (URLClassLoader) ucl.getParent()) {
			System.out.println("Class-loader " + n + " has "
					+ ucl.getURLs().length + " urls:");
			for (URL u : ucl.getURLs()) {
				System.out.println("  " + u);
			}
		}
	}
	public static HashMap<String,String> hideLoggingProperties() {
		String[] propsToSave = { "log4j.configuration", 
				                 "java.util.logging.config.file",
							     "java.util.logging.config.class",
							     "org.apache.uima.logger.class"};
		HashMap<String, String> savedPropsMap = new HashMap<String,String>();
		for (String prop : propsToSave) {
			String val = System.getProperty(prop);
			if (val != null) {
				savedPropsMap.put(prop,  val);
				System.getProperties().remove(prop);
				//System.out.println("!!!! Saved prop " + prop + " = " + val);
			}
		}
		return savedPropsMap;
	}

	public static void restoreLoggingProperties(HashMap<String,String> savedPropsMap) {
		for (String prop : savedPropsMap.keySet()) {
			System.setProperty(prop, savedPropsMap.get(prop));
			//System.out.println("!!!! Restored prop " + prop + " = " + System.getProperty(prop));
		}
	}
	public static String getPID(final String fallback) {
		// the following code returns '<pid>@<hostname>'
		String name = ManagementFactory.getRuntimeMXBean().getName();
		int pos = name.indexOf('@');

		if (pos < 1) {
			// pid not found
			return fallback;
		}

		try {
			return Long.toString(Long.parseLong(name.substring(0, pos)));
		} catch (NumberFormatException e) {
			// ignore
		}
		return fallback;
	}
	/**
	 * scan args for a particular arg, return the following token or the empty
	 * string if not found
	 * 
	 * @param id
	 *            the arg to search for
	 * @param args
	 *            the array of strings
	 * @return the following token, or a 0 length string if not found
	 */
	public static String getArg(String id, String[] args) {
		for (int i = 0; i < args.length; i++) {
			if (id.equals(args[i]))
				return (i + 1 < args.length) ? args[i + 1] : "";
		}
		return "";
	}

	/**
	 * scan args for a particular arg, return the following token(s) or the
	 * empty string if not found
	 * 
	 * @param id
	 *            the arg to search for
	 * @param args
	 *            the array of strings
	 * @return the following token, or a 0 length string array if not found
	 */
	public static String[] getMultipleArg(String id, String[] args) {
		String[] retr = {};
		for (int i = 0; i < args.length; i++) {
			if (id.equals(args[i])) {
				String[] temp = new String[retr.length + 1];
				for (int s = 0; s < retr.length; s++) {
					temp[s] = retr[s];
				}
				retr = temp;
				retr[retr.length - 1] = (i + 1 < args.length) ? args[i + 1]
						: null;
			}
		}
		return retr;
	}

	/**
	 * scan args for a particular arg, return the following token(s) or the
	 * empty string if not found
	 * 
	 * @param id
	 *            the arg to search for
	 * @param args
	 *            the array of strings
	 * @return the following token, or a 0 length string array if not found
	 */
	public static String[] getMultipleArg2(String id, String[] args) {
		String[] retr = {};
		for (int i = 0; i < args.length; i++) {
			if (id.equals(args[i])) {
				int j = 0;
				while ((i + 1 + j < args.length)
						&& !args[i + 1 + j].startsWith("-")) {
					String[] temp = new String[retr.length + 1];
					for (int s = 0; s < retr.length; s++) {
						temp[s] = retr[s];
					}
					retr = temp;
					retr[retr.length - 1] = args[i + 1 + j++];
				}
				return retr;
			}
		}
		return retr;
	}

}
