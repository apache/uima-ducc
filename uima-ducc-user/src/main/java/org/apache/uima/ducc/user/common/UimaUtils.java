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
package org.apache.uima.ducc.user.common;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.uima.UIMAFramework;
import org.apache.uima.resource.RelativePathResolver;
import org.apache.uima.resource.ResourceConfigurationException;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.impl.RelativePathResolver_impl;
import org.apache.uima.resource.metadata.ConfigurationParameter;
import org.apache.uima.resource.metadata.ConfigurationParameterDeclarations;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLInputSource;

public class UimaUtils {

	public static RelativePathResolver resolver = new RelativePathResolver_impl();

	public static URL getRelativePathWithProtocol(String aRelativePath)
			throws MalformedURLException {
		URL relativeUrl;
		try {
			relativeUrl = new URL(aRelativePath);
		} catch (MalformedURLException e) {
			relativeUrl = new URL("file", "", aRelativePath);
		}
		return relativeUrl;
	}

	public static ResourceSpecifier getResourceSpecifier(String resourceFile)
			throws Exception {
		return UIMAFramework.getXMLParser().parseResourceSpecifier(
				getXMLInputSource(resourceFile));
	}

	public static XMLInputSource getXMLInputSource(String resourceFile)
			throws InvalidXMLException {
		//
		// If the resourceFile ends in .xml then we look in the filesystem, end
		// of story.
		//
		// If not, then we turn it into a path by replacing . with / and
		// appending .xml.
		// We then have two places we need to look:
		// a) in the user's classpath directly as a file (not inside a jar), or
		// b) in the jar files in the user's classpath
		//

		try {
			resourceFile = Utils.resolvePlaceholderIfExists(resourceFile,
					System.getProperties());
			XMLInputSource in = null;
			if (resourceFile.endsWith(".xml")) {
				in = new XMLInputSource(resourceFile);
			} else {
				resourceFile = resourceFile.replace('.', '/') + ".xml";
				URL relativeURL = resolver
						.resolveRelativePath(getRelativePathWithProtocol(resourceFile));
				in = new XMLInputSource(relativeURL);
			}
			return in;
		} catch (NullPointerException npe) {
			throw new InvalidXMLException(
					InvalidXMLException.IMPORT_BY_NAME_TARGET_NOT_FOUND,
					new String[] { resourceFile });
		} catch (IOException e) {
			throw new InvalidXMLException(
					InvalidXMLException.IMPORT_FAILED_COULD_NOT_READ_FROM_URL,
					new String[] { resourceFile });
		}

	}

	public static ConfigurationParameter findConfigurationParameter(
			ConfigurationParameterDeclarations configurationParameterDeclarations,
			String name) {
		ConfigurationParameter retVal = null;
		for (ConfigurationParameter parameter : configurationParameterDeclarations
				.getConfigurationParameters()) {
			if (name.equals(parameter.getName())) {
				retVal = parameter;
				break;
			}
		}
		return retVal;
	}

	public static Object getOverrideValueObject(
			ConfigurationParameter configurationParameter, String value)
			throws ResourceConfigurationException {
		Object retVal = value;
		try {
			if (configurationParameter.getType().equals("Integer")) {
				retVal = Integer.parseInt(value);
			} else if (configurationParameter.getType().equals("Boolean")) {
				retVal = Boolean.parseBoolean(value);
			} else if (configurationParameter.getType().equals("Float")) {
				retVal = Float.parseFloat(value);
			}
		} catch (Throwable t) {
			throw new ResourceConfigurationException(t);
		}
		return retVal;
	}

}
