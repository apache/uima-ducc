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
package org.apache.uima.ducc.common.uima;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.uima.Constants;
import org.apache.uima.UIMAFramework;
import org.apache.uima.UIMARuntimeException;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.impl.AnalysisEngineDescription_impl;
import org.apache.uima.analysis_engine.metadata.FixedFlow;
import org.apache.uima.analysis_engine.metadata.FlowControllerDeclaration;
import org.apache.uima.analysis_engine.metadata.impl.FixedFlow_impl;
import org.apache.uima.analysis_engine.metadata.impl.FlowControllerDeclaration_impl;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.resource.RelativePathResolver;
import org.apache.uima.resource.ResourceConfigurationException;
import org.apache.uima.resource.ResourceCreationSpecifier;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.impl.RelativePathResolver_impl;
import org.apache.uima.resource.metadata.ConfigurationParameter;
import org.apache.uima.resource.metadata.ConfigurationParameterDeclarations;
import org.apache.uima.resource.metadata.ConfigurationParameterSettings;
import org.apache.uima.resource.metadata.Import;
import org.apache.uima.resource.metadata.impl.ConfigurationParameter_impl;
import org.apache.uima.resource.metadata.impl.Import_impl;
import org.apache.uima.resourceSpecifier.factory.DeploymentDescriptorFactory;
import org.apache.uima.resourceSpecifier.factory.ServiceContext;
import org.apache.uima.resourceSpecifier.factory.UimaASPrimitiveDeploymentDescriptor;
import org.apache.uima.resourceSpecifier.factory.impl.ServiceContextImpl;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLInputSource;


public class UimaUtils {
	public static final String FlowControllerKey="FixedFlowController";
	public static final String FlowControllerResourceSpecifier="ducc.flow-controller.specifier";
	public static RelativePathResolver resolver = new RelativePathResolver_impl();


	/**
	 * Creates and returns UIMA AS deployment descriptor from provided parts. It
	 * first creates UIMA AE aggregate descriptor and then creates UIMA AS
	 * primitive deployment descriptor for it.
	 * 
	 * @param name
	 *            - name of the UIMA AS service
	 * @param description
	 *            - description of the UIMA AS service
	 * @param brokerURL
	 *            - broker the UIMA AS service will connect to
	 * @param endpoint
	 *            - queue name of the UIMA AS service
	 * @param scaleup
	 *            - how many pipelines (threads) UIMA AS will deploy in the jvm
	 * 
	 * @param aeDescriptors
	 *            - vararg of AE descriptor paths
	 * 
	 * @return instance of UimaASPrimitiveDeploymentDescriptor
	 * 
	 * @throws Exception
	 */
	public static UimaASPrimitiveDeploymentDescriptor createUimaASDeploymentDescriptor(
			String name, String description, String brokerURL, String endpoint,
			int scaleup, String directory, String fname, String... aeDescriptors)
			throws Exception {
		List<List<String>> overrides = new ArrayList<List<String>>();
		return createUimaASDeploymentDescriptor(name, description, brokerURL,
				endpoint, scaleup, directory, fname, overrides, aeDescriptors);
	}

	/**
	 * Creates and returns UIMA AS deployment descriptor from provided parts. It
	 * first creates UIMA AE aggregate descriptor and then creates UIMA AS
	 * primitive deployment descriptor for it.
	 * 
	 * @param name
	 *            - name of the UIMA AS service
	 * @param description
	 *            - description of the UIMA AS service
	 * @param brokerURL
	 *            - broker the UIMA AS service will connect to
	 * @param endpoint
	 *            - queue name of the UIMA AS service
	 * @param scaleup
	 *            - how many pipelines (threads) UIMA AS will deploy in the jvm
	 * @param overrides
	 *            - a list containing overrides. Each component override is a
	 *            separate list containing strings with format <name>=<value>
	 * 
	 * @param aeDescriptors
	 *            - vararg of AE descriptor paths
	 * 
	 * @return instance of UimaASPrimitiveDeploymentDescriptor
	 * 
	 * @throws Exception
	 */
	public static UimaASPrimitiveDeploymentDescriptor createUimaASDeploymentDescriptor(
			String name, String description, String brokerURL, String endpoint,
			int scaleup, String directory, String fname, List<List<String>> overrides,
			String... aeDescriptors) throws Exception {
		// First create UIMA AE aggregate descriptor from provided aeDescriptor
		// paths
		AnalysisEngineDescription aed = createAggregateDescription((scaleup > 1),overrides,
				aeDescriptors);
		aed.getMetaData().setName(name);
		// Create a temporary file where AE descriptor will be saved
		//File tempAEDescriptorFile = null;
		File file = null;
		File dir = new File(directory);
		if (!dir.exists()) {
			dir.mkdir();
		}
		FileOutputStream fos = null;
		try {
			file = new File(dir, fname);//+"-uima-ae-descriptor-"+Utils.getPID()+".xml");
			fos = new FileOutputStream(file);
			aed.toXML(fos);
			
		} catch(Exception e) {
			throw e;
		} finally {
			if ( fos != null ) {
				fos.close();
			}
		}
		// Set up a context object containing service deployment information
		ServiceContext context = new ServiceContextImpl(name, description,
				file.getAbsolutePath().replace('\\', '/'),
				endpoint, brokerURL);
		// how many pipelines to deploy in the jvm
		context.setScaleup(scaleup);
		context.setProcessErrorThresholdCount(1);
		// Create UIMA AS deployment descriptor
		UimaASPrimitiveDeploymentDescriptor dd = DeploymentDescriptorFactory
				.createPrimitiveDeploymentDescriptor(context);
		return dd;
	}

	/**
	 * Creates UIMA aggregate AE description from provided parts. Takes as input
	 * vararg of AE descriptor paths for CM, AE, and CC. It creates an aggregate
	 * description with each component identified by its implementation class.
	 * The generated aggregate uses fixed flow.
	 * 
	 * @param descriptorPaths
	 *            - paths to uima component descriptors
	 * 
	 * @return - instantiated aggregate {@link AnalysisEngineDescription}
	 * 
	 * @throws Exception
	 */
	public static AnalysisEngineDescription createAggregateDescription(boolean multipleDeploymentsAllowed,
			String... descriptorPaths) throws Exception {
		List<List<String>> overrides = new ArrayList<List<String>>();
		return createAggregateDescription(multipleDeploymentsAllowed, overrides, descriptorPaths);
	}

//	public static URL resolveRelativePath(URL aRelativeUrl) {
//		// fallback on classloader
//		String f = aRelativeUrl.getFile();
//		URL absURL;
//		if (aRelativeUrl.getClass().getClassLoader() != null) {
//			absURL = aRelativeUrl.getClass().getClassLoader().getResource(f);
//		} else // if no ClassLoader specified (could be the bootstrap
//				// classloader), try the system
//		// classloader
//		{
//			absURL = ClassLoader.getSystemClassLoader().getResource(f);
//		}
//		if (absURL != null) {
//			return absURL;
//		}
//
//		// no file could be found
//		return null;
//	}

	public static URL getRelativePathWithProtocol(String aRelativePath)
			throws MalformedURLException {
		URL relativeUrl;
		try {
			relativeUrl = new URL(aRelativePath);
		} catch (MalformedURLException e) {
			relativeUrl = new URL("file", "", aRelativePath);
		}
		return relativeUrl;
		//		return resolveRelativePath(relativeUrl);
	}

	public static ResourceSpecifier getResourceSpecifier(String resourceFile) throws Exception {
		return UIMAFramework.getXMLParser().parseResourceSpecifier(getXMLInputSource(resourceFile));
    }

	public static XMLInputSource getXMLInputSource(String resourceFile)
        throws InvalidXMLException
    {
        //
        // If the resourceFile ends in .xml then we look in the filesystem, end of story.
        //
        // If not, then we turn it into a path by replacing . with / and appending .xml.
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
            throw new InvalidXMLException(InvalidXMLException.IMPORT_BY_NAME_TARGET_NOT_FOUND, new String[] {resourceFile});
        } catch (IOException e ) {
            throw new InvalidXMLException(InvalidXMLException.IMPORT_FAILED_COULD_NOT_READ_FROM_URL, new String[] {resourceFile});
        }

	}

	/**
	 * Creates UIMA aggregate AE description from provided parts. Takes as input
	 * vararg of AE descriptor paths for CM, AE, and CC. It creates an aggregate
	 * description with each component identified by its implementation class.
	 * The generated aggregate uses fixed flow.
	 * 
	 * @param overrides
	 *            - a list containing overrides. Each component override is a
	 *            separate list containing strings with format <name>=<value>
	 * 
	 * @param descriptorPaths
	 *            - paths to uima component descriptors
	 * 
	 * @return - instantiated aggregate {@link AnalysisEngineDescription}
	 * 
	 * @throws Exception
	 */
	public static AnalysisEngineDescription createAggregateDescription(
			boolean multipleDeploymentAllowed, List<List<String>> overrides, String... descriptorPaths)
			throws Exception {

		// create the descriptor and set configuration parameters
		AnalysisEngineDescription desc = new AnalysisEngineDescription_impl();
		resolver.setPathResolverClassLoader(desc.getClass().getClassLoader());
		desc.setFrameworkImplementation(Constants.JAVA_FRAMEWORK_NAME);
		desc.setPrimitive(false);
		ResourceSpecifier[] specifiers = new ResourceSpecifier[descriptorPaths.length];

		// Allow scale up
		desc.getAnalysisEngineMetaData().getOperationalProperties()
				.setMultipleDeploymentAllowed(multipleDeploymentAllowed);
		// Stores component names derived from implementation class
		List<String> flowNames = new ArrayList<String>();
		int inx = 0;
		// First produce ResourceSpecifiers from provided descriptors
		for (String aeDescription : descriptorPaths) {
			/*
			aeDescription = Utils.resolvePlaceholderIfExists(aeDescription,
					System.getProperties());
			XMLInputSource in = null;
			if (!aeDescription.endsWith(".xml")) {
				aeDescription = aeDescription.replace('.', '/') + ".xml";
				URL relativeURL = resolver.resolveRelativePath(getRelativePathWithProtocol(aeDescription));
//				URL relativeURL = resolveRelativePath(aeDescription);
				in = new XMLInputSource(relativeURL);
			} else {
				in = new XMLInputSource(aeDescription);
			}
			// XMLInputSource in = new XMLInputSource(aeDescription);
			ResourceSpecifier specifier = UIMAFramework.getXMLParser()
					.parseResourceSpecifier(in);
			specifiers[inx++] = specifier;
			*/
			specifiers[inx++] = getResourceSpecifier(aeDescription);
			// UimaClassFactory.produceResourceSpecifier(aeDescription);
		}

		for (String aeDescription : descriptorPaths) {
			Import descriptorImport = new Import_impl();
			// If user provides a descriptor with .xml at the end, assume he
			// wants import by location
			if (aeDescription.endsWith(".xml")) {
				aeDescription = Utils.resolvePlaceholderIfExists(aeDescription,
						System.getProperties());
				if (!aeDescription.startsWith("file:")) {
					aeDescription = "file:" + aeDescription;
				}
				descriptorImport.setLocation(aeDescription);
			} else {
				// uima import by name expects dot separated path as in
				// a.b.descriptor and no .xml at the end
				descriptorImport.setName(aeDescription);
			}
			String key = new String(aeDescription);
			if (key.startsWith("file:")) {
				key = key.substring(5); // skip "file:"
			}
			if (key.endsWith(".xml")) {
				key = key.substring(0, key.indexOf(".xml")); // strip ".xml"
			}
			// preprocess the ae descriptor name to replace "/" and
			// "\" with ".". We will use the ae
			// descriptor name as AE key in the aggregate
			if (key.indexOf("/") != -1) {
				key = key.replaceAll("/", ".");
			}
			if (key.indexOf("\\") != -1) {
				key = key.replaceAll("\\\\", ".");
			}
			key = key.substring(key.lastIndexOf(".") + 1);
			desc.getDelegateAnalysisEngineSpecifiersWithImports().put(key,
					descriptorImport);
			flowNames.add(key);

		}
		String fcsn;
		if ( (fcsn = DuccPropertiesResolver.getInstance().getProperty(FlowControllerResourceSpecifier)) != null ) {
			FlowControllerDeclaration fcd = new FlowControllerDeclaration_impl();
			desc.setFlowControllerDeclaration(fcd);
			fcd.setImport(new Import_impl());		
			fcd.setKey(FlowControllerKey);
			fcd.getImport().setName(fcsn);
		}
		
		FixedFlow fixedFlow = new FixedFlow_impl();
		fixedFlow.setFixedFlow(flowNames.toArray(new String[flowNames.size()]));
		desc.getAnalysisEngineMetaData().setFlowConstraints(fixedFlow);
		addOverrides(overrides, desc, specifiers, flowNames);

		return desc;
	}
	
	public static ConfigurationParameter findConfigurationParameter(ConfigurationParameterDeclarations configurationParameterDeclarations, String name) {
		ConfigurationParameter retVal = null;
		for (ConfigurationParameter parameter : configurationParameterDeclarations.getConfigurationParameters()) {
			if (name.equals(parameter.getName())) {
				retVal = parameter;
				break;
			}
		}
		return retVal;
	}
	
	public static Object getOverrideValueObject(ConfigurationParameter configurationParameter, String value) throws ResourceConfigurationException {
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

	private static void addOverrides(List<List<String>> overrides,
			AnalysisEngineDescription desc, ResourceSpecifier[] specifiers,
			List<String> flowNames) throws Exception {

		ConfigurationParameterDeclarations aggregateDeclarations = desc
				.getAnalysisEngineMetaData()
				.getConfigurationParameterDeclarations();
		ConfigurationParameterSettings aggregateSetttings = desc
				.getAnalysisEngineMetaData()
				.getConfigurationParameterSettings();
		int indx = 0;
		for (List<String> componentOverrides : overrides) {
			if ( specifiers[indx] instanceof ResourceCreationSpecifier ) {
				addComponentOverrides(flowNames.get(indx), componentOverrides,
						(ResourceCreationSpecifier) specifiers[indx],
						aggregateDeclarations, aggregateSetttings);
			}
			indx++;
		}
		
	}

	/**
	 * Modifies aggregate descriptor by adding component specific overrides.
	 * 
	 * @param key
	 *            - component key
	 * @param componentOverrides
	 *            - List of override params where element is expressed as String
	 *            with format <name>=<value>
	 * @param specifier
	 *            - component resource specifier
	 * @param aggregateDeclarations
	 *            - aggregate ConfigurationParameterDeclarations
	 * @param aggregateSetttings
	 *            - aggregate ConfigurationParameterSettings
	 */
	private static void addComponentOverrides(String key,
			List<String> componentOverrides,
//			AnalysisEngineDescription specifier,
			ResourceCreationSpecifier specifier,
			ConfigurationParameterDeclarations aggregateDeclarations,
			ConfigurationParameterSettings aggregateSetttings) throws Exception {

		if (componentOverrides == null || componentOverrides.isEmpty()) { // no
																			// overrides
			return; // nothing to do
		}
		processOverrides(key, componentOverrides,
			specifier, aggregateDeclarations,
			//	(ResourceCreationSpecifier) specifier, aggregateDeclarations,
				aggregateSetttings);

	}

	private static void processOverrides(String key,
			List<String> componentOverrides,
			ResourceCreationSpecifier specifier,
			ConfigurationParameterDeclarations aggregateDeclarations,
			ConfigurationParameterSettings aggregateSetttings) throws Exception {
		// Process overrides
		for (String cmOverride : componentOverrides) {
			System.out.println(".... Processing Override:"+cmOverride);
			// each override is expressed as <name>=<value> pair, so split on
			// the first '=' found ... in case the value contains an '='
			String[] nvp = cmOverride.split("=", 2);
			// Fetch component parameter declarations to get the primitive type
			// of the parameter
			ConfigurationParameterDeclarations componentParameterDeclarations = specifier
					.getMetaData().getConfigurationParameterDeclarations();
			// Iterate over component parameter declarations looking to find one
			// with the same name
			// as provided in the override. On match, add an override to the
			// aggregate and preserve
			// the type defined for the parameter in the component descriptor.
			// If no match, throw
			// an exception
			boolean found = false;
			for (ConfigurationParameter parameter : componentParameterDeclarations
					.getConfigurationParameters()) {
				if (nvp[0].equals(parameter.getName())) {
					addParam(key, nvp, parameter, aggregateDeclarations);
					addParamValue(nvp, parameter, aggregateSetttings);
					found = true;
					break;
				}
			}
			if (!found) {
				throw new UIMARuntimeException(
						new InvalidOverrideParameterException(
								"Override Parameter:"
										+ nvp[0]
										+ " is not defined for the component with key: "
										+ key));
			}
		}

	}

	/**
	 * Adds parameter to aggregate ConfigurationParameterDeclarations.
	 * 
	 * @param key
	 *            - component key
	 * @param nvp
	 *            - override name value pair
	 * @param parameter
	 *            - matching ConfigurationParameter instance from component
	 *            descriptor or null
	 * @param aggregateDeclarations
	 *            - aggregate ConfigurationParameterDeclarations instance
	 */
	private static void addParam(String key, String[] nvp,
			ConfigurationParameter parameter,
			ConfigurationParameterDeclarations aggregateDeclarations) {
		ConfigurationParameter cfgParam = new ConfigurationParameter_impl();
		cfgParam.setName(nvp[0]);
		if (parameter == null) { // component descriptor doesnt contain a
									// parameter provided in the override list.
									// Default to String
			cfgParam.setType("String"); // create String param as default
		} else {
			cfgParam.setType(parameter.getType());
		}
//		if ( key.equals(FlowControllerKey)) {
//			cfgParam.addOverride(key + "/ActionAfterCasMultiplier");
//		} else {
//			cfgParam.addOverride(key + "/" + nvp[0]);
//		}
		cfgParam.addOverride(key + "/" + nvp[0]);
		aggregateDeclarations.addConfigurationParameter(cfgParam);

	}

	private static void addParamValue(String[] nvp,
			ConfigurationParameter parameter,
			ConfigurationParameterSettings aggregateSettings) {

		Object value = nvp[1]; // default is String value
		if (parameter != null) {
			if (parameter.getType().equals("Integer")) {
				value = Integer.parseInt(nvp[1]);
			} else if (parameter.getType().equals("Boolean")) {
				value = Boolean.parseBoolean(nvp[1]);
			} else if (parameter.getType().equals("Float")) {
				value = Float.parseFloat(nvp[1]);
			}
			aggregateSettings.setParameterValue(nvp[0], value);
		} else {
			aggregateSettings.setParameterValue(nvp[0], value);
		}
	}

}
