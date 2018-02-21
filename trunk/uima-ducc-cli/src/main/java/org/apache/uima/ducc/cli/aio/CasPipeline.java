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
package org.apache.uima.ducc.cli.aio;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineManagement;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.ducc.cli.aio.AllInOne.MsgHandler;
import org.apache.uima.ducc.common.uima.UimaHelper;
import org.apache.uima.ducc.user.common.QuotedOptions;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.FsIndexDescription;
import org.apache.uima.resource.metadata.TypePriorities;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.XMLInputSource;

public class CasPipeline {

	public static String cid = CasPipeline.class.getSimpleName();
	
	protected MsgHandler mh;
	protected Properties properties = new Properties();
	
	private AnalysisEngineDescription aed = null;
	AnalysisEngine ae = null;

	private CAS cas = null;
	
	public CasPipeline(Properties properties, AllInOne.MsgHandler mh) {
	    this.properties = properties;
		this.mh = mh;
	}
	
	private ArrayList<String> toArrayList(String overrides) {
		// To match other lists tokenize on blanks & strip any quotes around values.
        ArrayList<String> list = QuotedOptions.tokenizeList(overrides, true);
		return list;
	}
	
	private File getFile(String descriptor) {
		String mid = "getFile";
		File file;
		if(descriptor.endsWith(".xml")) {
			mh.frameworkDebug(cid, mid, descriptor);
			file = new File(descriptor);
		}
		else {
			String relativePath = descriptor.replace('.', '/')+".xml";
			URL url = getClass().getClassLoader().getResource(relativePath);
			if(url == null) {
				throw new IllegalArgumentException(relativePath+" not found in classpath");
			}
			mh.frameworkDebug(cid, mid, url.getFile());
			file = new File(url.getFile());
		}
		return file;
	}
	
	private void initializeByDD() throws Exception {
		String mid = "initializeByDD";
		String dd = properties.getProperty(AllInOne.ProcessDD);
		File ddFile = getFile(dd);
		DDParser ddParser = new DDParser(ddFile);
		String ddImport = ddParser.getDDImport();
		mh.frameworkDebug(cid, mid, ddImport);
		File uimaFile = getFile(ddImport);
		XMLInputSource xis = new XMLInputSource(uimaFile);
		ResourceSpecifier specifier = UIMAFramework.getXMLParser().parseResourceSpecifier(xis);
	    ae = UIMAFramework.produceAnalysisEngine(specifier);
	}
	
	private void initializeByParts() throws Exception {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		List<List<String>> overrides = new ArrayList<List<String>>();
		List<String> descriptors = new ArrayList<String>();
		String cmDescriptor = properties.getProperty(AllInOne.ProcessDescriptorCM);
		if(cmDescriptor != null) {
			ArrayList<String> cmOverrides = toArrayList(properties.getProperty(AllInOne.ProcessDescriptorCMOverrides));
			overrides.add(cmOverrides);
			descriptors.add(cmDescriptor);
		}
		String aeDescriptor = properties.getProperty(AllInOne.ProcessDescriptorAE);
		if(aeDescriptor != null) {
			ArrayList<String> aeOverrides = toArrayList(properties.getProperty(AllInOne.ProcessDescriptorAEOverrides));
			overrides.add(aeOverrides);
			descriptors.add(aeDescriptor);
		}
		String ccDescriptor = properties.getProperty(AllInOne.ProcessDescriptorCC);
		if(ccDescriptor != null) {
			ArrayList<String> ccOverrides = toArrayList(properties.getProperty(AllInOne.ProcessDescriptorCCOverrides));
			overrides.add(ccOverrides);
			descriptors.add(ccDescriptor);
		}
		if(descriptors.size() == 3) {
			aed = UimaHelper.createAggregateDescription(false, overrides, descriptors.get(0), descriptors.get(1), descriptors.get(2));
		}
		else if(descriptors.size() == 2) {
			aed = UimaHelper.createAggregateDescription(false, overrides, descriptors.get(0), descriptors.get(1));
		}
		else if(descriptors.size() == 1) {
			aed = UimaHelper.createAggregateDescription(false, overrides, descriptors.get(0));
		}
		System.out.println("Created descriptor:");
		aed.toXML(System.out);
		System.out.println("");
		aed.toXML(baos);
		ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
		File file = null;
		XMLInputSource xis = new XMLInputSource(bais, file);
		ResourceSpecifier specifier = UIMAFramework.getXMLParser().parseResourceSpecifier(xis);
	    ae = UIMAFramework.produceAnalysisEngine(specifier);
	}
	
	public void initialize() throws Exception {
		String dd = properties.getProperty(AllInOne.ProcessDD);
		if(dd != null) {
			initializeByDD();
		}
		else {
			initializeByParts();
		}
	}
	
	public CAS process(CAS cas) throws AnalysisEngineProcessException {
		ae.process(cas);
		return cas;
	}
	
	public void destroy() {
	    ae.destroy();
	}
	
	public CAS getEmptyCas() throws ResourceInitializationException {
		// Reuse same CAS for each request
		if (cas != null) {
			cas.reset();
		} else {
	        TypePriorities ae_tp = ae.getProcessingResourceMetaData().getTypePriorities();
	        TypeSystemDescription ae_tsd = ae.getProcessingResourceMetaData().getTypeSystem();
	        FsIndexDescription[] ae_fid = ae.getProcessingResourceMetaData().getFsIndexes();
			//	Use class level locking to serialize access to CasCreationUtils
			//  Only one thread at the time can create a CAS. UIMA uses lazy
			//  initialization approach which can cause NPE when two threads
			//  attempt to initialize a CAS. 
			synchronized( CasCreationUtils.class) {
				cas = CasCreationUtils.createCas(ae_tsd, ae_tp, ae_fid);
			}
		}
		return cas;
	}
	
	public void dumpStatistics(PrintStream out) {
		out.println("");
		out.println("+---------------------------+");
		out.println("| UIMA Component Statistics |");
		out.println("+---------------------------+");
		out.println("");
		AnalysisEngineManagement aem = ae.getManagementInterface();
	    dumpComponentStatistics(out, 0, aem);
	}

	private static void dumpComponentStatistics(PrintStream out, int level, AnalysisEngineManagement aem) {
		String indent = "";
	    for (int i = 0; i < level; i++) {
	    	indent += "  ";
	    }
	    out.println(indent+aem.getName()+": "+aem.getAnalysisTime()+"ms, ");
	    for (AnalysisEngineManagement childAem : (Iterable<AnalysisEngineManagement>) (aem.getComponents().values())) {
	    	dumpComponentStatistics(out, level+1, childAem);
	    }
	}
}
