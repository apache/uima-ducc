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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.ducc.user.common.UimaUtils;
import org.apache.uima.ducc.user.jp.UimaASProcessContainer;
import org.apache.uima.internal.util.XMLUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class DeployableGenerator {
	
	private String userLogDir = null;
  private Document doc;
  private String registryURL;
  
	public DeployableGenerator(String userLogDir) {
		setUserLogDir(userLogDir);
	}
	
	private void setUserLogDir(String value) {
		userLogDir = value;
	}
	
	public String generateAe(IDuccGeneratorUimaAggregate aggregateConfiguration, String jobId, boolean createUniqueFilename) throws Exception {
		List<String> descriptorPaths = new ArrayList<String>();
		List<List<String>> overrides = new ArrayList<List<String>>();
		for( IDuccGeneratorUimaAggregateComponent component: aggregateConfiguration.getComponents()) {
			descriptorPaths.add(component.getDescriptor());
			overrides.add(component.getOverrides());
		}
		String aed = createAED(
		    aggregateConfiguration.getFlowController(),
		    aggregateConfiguration.getThreadCount(), 
		    userLogDir,
		    createUniqueFilename ? null : jobId+"-"+"uima-ae-descriptor"+".xml",
		    overrides, 
		    descriptorPaths.toArray(new String[descriptorPaths.size()])
			);
		return aed;
	}

	private static String createAED (
			String flowController,
			int scaleup, 
			String directory, 
			String fname, 
			List<List<String>> overrides,
			String... aeDescriptors) throws Exception {
		
		AnalysisEngineDescription aed = UimaUtils.createAggregateDescription(flowController, (scaleup > 1), overrides, aeDescriptors);
		aed.getMetaData().setName("DUCC.job");
		File dir = new File(directory);
		if (!dir.exists()) {
			dir.mkdir();
		}
		FileOutputStream fos = null;
		try {
		  File file = File.createTempFile("uima-ae-", ".xml", dir);
			fos = new FileOutputStream(file);
			aed.toXML(fos);
			if (fname == null) {     // Use the unique name
			  deleteOnExitCheck(file);
			  return file.getAbsolutePath();
			}
			// Use the atomic Files.move method (reportedly better than File:renameTo)
			Path source = file.toPath();
			Path target = source.resolveSibling(fname);
			Files.move(source,  target, StandardCopyOption.ATOMIC_MOVE);
			return target.toString();
		} 
		catch(Exception e) {
			throw e;
		} 
		finally {
			if( fos != null ) {
				fos.close();
			}
		}
	}
	
	/*
	 * This method is used by the JD to convert a deployment descriptor's inputQueue element
	 * to make it suitable for the JP's internal broker.
	 * It is also used by the JP code since when running as a "pull" service it will be given an unconverted DD 
	 */
	public String generateDd(IDuccGeneratorUimaReferenceByName configuration, String jobId, Boolean createUniqueFilename) throws Exception {
		//  Create DOM from the DD ... file or class-like name
		String location = configuration.getReferenceByName();
    org.apache.uima.util.XMLInputSource xmlin = UimaUtils.getXMLInputSource(location);  // Reads from FS or classpath
    
    DocumentBuilderFactory dbFactory = XMLUtils.createDocumentBuilderFactory();
    DocumentBuilder db = dbFactory.newDocumentBuilder();
    
    //DocumentBuilder db = DocumentBuilderFactory.newInstance().newDocumentBuilder();
    doc = db.parse(xmlin.getInputStream());
		
    // Create converted descriptor if input is not a file or if endpoint or broker wrong
    boolean createDescriptor = ! location.endsWith(".xml");
    
		//  locate the <inputQueue node within the xml ... should be only one
		NodeList nodes = doc.getElementsByTagName("inputQueue");
		Element element;
    if (nodes.getLength() > 0) {
      element = (Element) nodes.item(0);
      // Check if the attributes are correct
      String expected = "${" + UimaASProcessContainer.queuePropertyName + "}";
      if ( ! element.getAttribute("endpoint").equals(expected)) {
        element.setAttribute("endpoint", expected);
        createDescriptor = true;
      }
      expected = "${" + UimaASProcessContainer.brokerPropertyName + "}";
      if ( ! element.getAttribute("brokerURL").equals(expected)) {
        element.setAttribute("brokerURL", expected);
        createDescriptor = true;
      }
      // May specify the registry via an unsupported attribute
      registryURL = element.getAttribute("registryURL");  // Defaults to an empty string
      element.removeAttribute("registryURL");
    } else {
      throw new Exception("Invalid DD-" + configuration.getReferenceByName()
              + ". Missing required element <inputQueue ...");
    }
    
    //	Return the original descriptor or the converted one if necessary
		return createDescriptor ? writeDDFile(xml2String(doc), jobId, createUniqueFilename) : location;
	}

  /* 
   *  Deduce the scaleout for a deployment descriptor.
   *  If a top-level non-AS deployment check for a scaleout setting.
   *  Otherwise use the caspool size, with a default of 1
   */
	public int getScaleout() {
	  if (doc == null) {  // Not a DD ?
	    return 1;
	  }

    String soValue = "";
    NodeList nodes = doc.getElementsByTagName("analysisEngine");
    if (nodes.getLength() > 0) {
      Element aeElement = (Element) nodes.item(0);
      String async = aeElement.getAttribute("async");
      // If async is omitted the default is false if there are no delegates
      if (async.isEmpty()) {
        if (aeElement.getElementsByTagName("delegates").getLength() == 0) {
          async = "false";
        } 
      }
      // If async is false a scaleout setting can override the caspool size
      if (async.equals("false")) {
        nodes = aeElement.getElementsByTagName("scaleout");
        if (nodes.getLength() > 0) {
          Element soElement = (Element) nodes.item(0);
          soValue = soElement.getAttribute("numberOfInstances");
        }
      }
    }
    
    if (soValue.isEmpty()) {
      nodes = doc.getElementsByTagName("casPool");
      if (nodes.getLength() > 0) {
        Element cpElement = (Element) nodes.item(0);
        soValue = cpElement.getAttribute("numberOfCASes");
      }
    }
    
    return soValue.isEmpty() ? 1 : Integer.parseInt(soValue);
	}
	
	public String getRegistryUrl() {
	  return registryURL;
	}
	
	private String xml2String(Document xmlDoc) throws Exception {
		StringWriter writer = null;

		DOMSource domSource = new DOMSource(xmlDoc.getDocumentElement());
		
		writer = new StringWriter();

		StreamResult streamResult = new StreamResult(writer);
		TransformerFactory factory =
		    XMLUtils.createTransformerFactory();
		
		//TransformerFactory factory = TransformerFactory.newInstance();
		Transformer transformer = factory.newTransformer();
		transformer.transform(domSource, streamResult);

		StringBuffer serializedDD = writer.getBuffer();
		return serializedDD.toString();
	}
	
	private String writeDDFile(String content, String jobId, boolean createUniqueFilename) throws Exception {
		File dir = new File(userLogDir);
		if ( !dir.exists()) {
			dir.mkdir();
		}
		//	compose the file name from a basename (from ducc.properties), constant (-uima-as.dd-) and PID
		// Create as a temp file then rename atomically (unless the JP wants a unique temporary file)
		BufferedWriter out = null;
		try {
			File file = File.createTempFile("uima-as-dd-", ".xml", dir);
			out = new BufferedWriter(new FileWriter(file));
			out.write(content);
			if (createUniqueFilename) {
			  deleteOnExitCheck(file);
			  return file.getAbsolutePath();
			}
			Path source = file.toPath();
			Path target = source.resolveSibling(jobId+"-uima-as-dd.xml");
			Files.move(source,  target, StandardCopyOption.ATOMIC_MOVE);
			return target.toString();
		} catch( Exception e) {
			throw e;
		} finally {
			if ( out != null ) {
				out.close();
			}
		}
	}
	
	// Don't delete descriptors if this environment variable is set
	// (Can't put the key in IDuccUser as that is in the common project)
	private static void deleteOnExitCheck(File f) {
    if (System.getenv("DUCC_KEEP_TEMPORARY_DESCRIPTORS") == null) {
      f.deleteOnExit();
    }
	}
}
