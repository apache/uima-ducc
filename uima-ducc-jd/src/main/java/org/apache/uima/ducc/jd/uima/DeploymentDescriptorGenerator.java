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
package org.apache.uima.ducc.jd.uima;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.ducc.common.utils.XStreamUtils;
import org.apache.uima.ducc.transport.event.common.IDuccUimaAggregate;
import org.apache.uima.ducc.transport.event.common.IDuccUimaAggregateComponent;
import org.apache.uima.ducc.transport.event.common.IDuccUimaDeployableConfiguration;
import org.apache.uima.ducc.transport.event.common.IDuccUimaDeploymentDescriptor;
import org.apache.uima.resourceSpecifier.factory.UimaASPrimitiveDeploymentDescriptor;
import org.apache.uima.util.XMLInputSource;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

/**
 * Generates UIMA AS deployment descriptor. Takes in as input implementations of 
 * IDuccUimaDeployableConfiguration interface creates a DD. Currently two implementations
 * of IDuccUimaDeployableConfiguration are supported:
 * 
 * 1) IDuccUimaDeploymentDescriptor - this contains a reference to an existing DD that a 
 *    user provided when submitting a job. In this case, the code overrides a queue name
 *    and a broker url and new descriptors are generated. 
 * 
 * 2) IDuccUimaAggregate - this contains a list of delegate components that must first
 *    be assembled into UIMA Aggregate descriptor. After generating that descriptor the
 *    code generates DD for it.
 */
public class DeploymentDescriptorGenerator {

	private DuccLogger logger; // logger to use
	private String duccComponentName; // DUCC component using this class (JD)
	private String userLogDir;  // where to write both descriptors
	
	public DeploymentDescriptorGenerator(String componentName, DuccLogger logger, String userLogDir) {
		this.logger = logger;
		//	Type of ducc component this is using this class (ex.JD)
		this.duccComponentName = componentName;
		this.userLogDir = userLogDir;
	}
	public String generate(String serializedDeployableConfiguration, String jobId) throws Exception {
		IDuccUimaDeployableConfiguration deployableConfiguration =
				(IDuccUimaDeployableConfiguration)XStreamUtils.unmarshall(serializedDeployableConfiguration);
		return generate(deployableConfiguration, jobId);
	}

	public String getComponentName() {
		return duccComponentName;
	}
	
	/**
	 * Generates deployment descriptor for DUCC JPs.
	 * 
	 * @param configuration - deployment configuration for the deployment descriptor
	 * @return - absolute path to the generated deployment descriptor
	 * @throws Exception - failure
	 */
	public String generate(IDuccUimaDeployableConfiguration configuration, String jobId)
			throws Exception {
		String methodName="generate";
		if ( configuration instanceof IDuccUimaDeploymentDescriptor) {
			logger.debug(methodName, null, "DUCC Service Wrapper Generates Deployment Descriptor Based on DD Provided by a User:"+((IDuccUimaDeploymentDescriptor)configuration).getDeploymentDescriptorPath());
			// User provided descriptor is used as a basis for generating the actual DD with overriden 
			// Broker URL and queue name. 
			return generateDeploymentDescriptor((IDuccUimaDeploymentDescriptor)configuration, jobId);
		} else if ( configuration instanceof IDuccUimaAggregate) {
			logger.debug(methodName, null,"DUCC Service Wrapper Generating UIMA AS Deployment Descriptor");
			// First create UIMA aggregate descriptor and then a deployment descriptor.
			return generateDeploymentDescriptor((IDuccUimaAggregate)configuration, jobId);
		} else throw new Exception("Invalid IDuccUimaDeployableConfiguration. Expected IDuccUimaAggregate or IDuccUimaDeploymentDescriptor, but received "+configuration.getClass().getName());
	}
	/**
	 * Writes a given content into a file in a given directory 
	 * 
	 * @param content - content in the file
	 * @return - absolute path to the file created
	 * 
	 * @throws Exception
	 */
	private String writeDDFile(String content, String jobId) throws Exception {
		File dir = new File(userLogDir);
		if ( !dir.exists()) {
			dir.mkdir();
		}
		//	compose the file name from a basename (from ducc.properties), constant (-uima-as.dd-) and PID
		BufferedWriter out = null;
		try {
			//	using PID of the ducc component process in the DD file name
			File file = new File(dir, jobId+"-uima-as-dd-"+Utils.getPID()+".xml");
			out = new BufferedWriter(new FileWriter(file));
			out.write(content);
			out.flush();
			return file.getAbsolutePath();
		} catch( Exception e) {
			throw e;
		} finally {
			if ( out != null ) {
				out.close();
			}
		}
	}
	/**
	 * Modifies user provided deployment descriptor by changing queue name and broker URL and
	 * writes it to a new deployment descriptor file.
	 *  
	 * @param dd - user specified deployment descriptor
	 * @return - absolute path to the generated deployment descriptor
	 * @throws Exception
	 */
	public String generateDeploymentDescriptor(IDuccUimaDeploymentDescriptor dd, String jobId) throws Exception {
	  //  parse DD into DOM 
	  Document doc = parse(dd.getDeploymentDescriptorPath());
	  //  locate the <inputQueue node within the xml
    NodeList nodes = doc.getElementsByTagName("inputQueue");
    //  should only be one such node
    if ( nodes.getLength() > 0 ) {
       Element element = (Element) nodes.item(0);
       // replace queue name
       element.setAttribute("endpoint", "ducc.jd.queue."+jobId);
       // replace broker URL
       element.setAttribute("brokerURL", System.getProperty("ducc.broker.url"));
    } else {
      throw new Exception("Invalid DD-"+dd.getDeploymentDescriptorPath()+". Missing required element <inputQueue ...");
    }
    //
		//	write the adjusted deployment descriptor to the user log dir where dd2spring will
		//  pick it up from
    //
		return writeDDFile(xml2String(doc), jobId);
	}
	/**
	 * Parses xml into DOM
	 * 
	 * @param location - name of the xml file to parse
	 * @return DOM
	 * @throws Exception
	 */
	private Document parse(String location ) throws Exception {
    //  Resolve the descriptor either by name or by location
    XMLInputSource xmlin = 
            UimaUtils.getXMLInputSource(location); // this guy does all the magic
    //  Parse the descriptor into DOM
    DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    return db.parse(xmlin.getInputStream());
	}
	/**
	 * Serializes Deployment Descriptor from DOM to a String
	 * @param xmlDoc - xml to serialize
	 * @return - serialized DD
	 * @throws Exception
	 */
	private String xml2String(Document xmlDoc) throws Exception {
    StringWriter writer = null;
    
    DOMSource domSource = new DOMSource(xmlDoc.getDocumentElement());
    writer = new StringWriter();
    
    StreamResult streamResult = new StreamResult(writer);
    TransformerFactory factory = TransformerFactory.newInstance();
    Transformer transformer = factory.newTransformer();
    transformer.transform(domSource, streamResult);
    
    StringBuffer serializedDD = writer.getBuffer();
    return serializedDD.toString();
  }
	/**
	 * Creates and returns absolute path to the UIMA AS deployment descriptor.
	 * 
	 * @param aggregateConfiguration - configuration object containing UIMA aggregate delegate
	 *        configuration and overrides. 
	 * 
	 * @return - absolute path to the generated deployment descriptor
	 * @throws Exception
	 */
	private String generateDeploymentDescriptor(IDuccUimaAggregate aggregateConfiguration, String jobId ) throws Exception {
		
		List<String> descriptorPaths = new ArrayList<String>();
		List<List<String>> overrides = new ArrayList<List<String>>();
		for( IDuccUimaAggregateComponent component: aggregateConfiguration.getComponents()) {
			descriptorPaths.add(component.getDescriptor());
			overrides.add(component.getOverrides());
		}
		UimaASPrimitiveDeploymentDescriptor dd = 
				UimaUtils.createUimaASDeploymentDescriptor(
				    aggregateConfiguration.getName(), 
				    aggregateConfiguration.getDescription(), 
				    aggregateConfiguration.getBrokerURL(), 
				    aggregateConfiguration.getEndpoint(),	
				    aggregateConfiguration.getThreadCount(), 
				    userLogDir,
				    jobId+"-uima-ae-descriptor-"+Utils.getPID()+".xml",
					overrides, 
					descriptorPaths.toArray(new String[descriptorPaths.size()])
					);
		return writeDDFile(dd.toXML(), jobId);
	}

}
