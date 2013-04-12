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

import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.ducc.cli.IUiOptions.UiOption;
import org.apache.uima.ducc.common.uima.UimaUtils;
import org.apache.uima.resource.ResourceConfigurationException;
import org.apache.uima.resource.ResourceCreationSpecifier;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.ConfigurationParameter;
import org.apache.uima.resource.metadata.ConfigurationParameterDeclarations;
import org.apache.uima.resource.metadata.ConfigurationParameterSettings;
import org.apache.uima.resource.metadata.FsIndexDescription;
import org.apache.uima.resource.metadata.TypePriorities;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.Progress;
import org.apache.uima.util.XMLInputSource;
import org.apache.uima.util.XMLParser;

public class CasGenerator {

	public static String cid = CasGenerator.class.getSimpleName();
	
	protected IMessageHandler mh = new MessageHandler();
	protected Properties properties = new Properties();

	private CollectionReader cr;
	private TypePriorities cr_tp;
	private TypeSystemDescription cr_tsd;
	private FsIndexDescription[] cr_fid;
	private Properties cr_properties = new Properties();
	
	private int total;
	
	public CasGenerator(Properties properties, IMessageHandler mh) {
		if(properties != null) {
			this.properties = properties;
		}
		if(mh != null) {
			this.mh = mh;
		}
	}
	
	public void initialize() throws InvalidXMLException, ResourceConfigurationException, ResourceInitializationException {
		String mid = "initialize";
		mh.frameworkTrace(cid, mid, "enter");
		String crDescriptor = properties.getProperty(UiOption.DriverDescriptorCR.pname());
		String crOverrides = properties.getProperty(UiOption.DriverDescriptorCROverrides.pname());
		XMLParser xmlParser = UIMAFramework.getXMLParser();
		XMLInputSource in = null;
		try {
			in = UimaUtils.getXMLInputSource(crDescriptor);
		}
		catch (InvalidXMLException e) {
			mh.error(e);
			throw e;
		}
		ResourceSpecifier crrs = xmlParser.parseCollectionReaderDescription(in);
		// CR overrides
        ResourceCreationSpecifier specifier = (ResourceCreationSpecifier) crrs;
        ConfigurationParameterDeclarations configurationParameterDeclarations = specifier.getMetaData().getConfigurationParameterDeclarations();
        ConfigurationParameterSettings cps = specifier.getMetaData().getConfigurationParameterSettings();
        if(crOverrides != null) {
            Plist plist = new Plist(crOverrides);
            TreeMap<String,String> map = plist.getParameterMap();
            Iterator<String> iterator = map.keySet().iterator();
            while(iterator.hasNext()) {
                String name = iterator.next();
                String value = map.get(name);
                String message = "config param name:"+name+" "+"value:"+value;
                mh.frameworkDebug(cid, mid, message);
                ConfigurationParameter configurationParameter = UimaUtils.findConfigurationParameter(configurationParameterDeclarations, name);
                Object object = UimaUtils.getOverrideValueObject(configurationParameter, value);
                cps.setParameterValue(name, object);
            }
        }
        cr = UIMAFramework.produceCollectionReader(crrs);
        // Change the heap size to reduce CAS size. The size here depends on what we intend to put into the CAS. 
        // If its just a pointer to data (like question id), the size of the heap can be reduced even more.
        cr_properties.setProperty(UIMAFramework.CAS_INITIAL_HEAP_SIZE, "1000");
        // Save CR type system. It will be used to initialize CASes later, in getEmptyCas().
        cr_tp = cr.getProcessingResourceMetaData().getTypePriorities();
        cr_tsd = cr.getProcessingResourceMetaData().getTypeSystem();
        cr_fid = cr.getProcessingResourceMetaData().getFsIndexes();
        initTotal();
        mh.frameworkInfo(cid, mid, "total:"+getTotal());
        mh.info("total:"+getTotal());
		mh.frameworkTrace(cid, mid, "exit");
	}
	
	public Progress[] getProgressArray() {
		String mid = "getProgressArray";
		mh.frameworkTrace(cid, mid, "enter");
		Progress[] retVal;
		synchronized(cr) {
			retVal = cr.getProgress();
		}
		mh.frameworkTrace(cid, mid, "exit");
		return retVal;
	}
	
	public Progress getProgress() {
		String mid = "getProgress";
		mh.frameworkTrace(cid, mid, "enter");
		Progress progress = null;
		Progress[] progressArray = getProgressArray();
		if(progressArray != null) {
			progress = progressArray[0];
		}
		mh.frameworkTrace(cid, mid, "exit");
		return progress;
	}
	
	private void initTotal() {
		String mid = "initTotal";
		mh.frameworkTrace(cid, mid, "enter");
		Progress progress = getProgress();
		if(progress != null) {
			total = (int)progress.getTotal();
		}
		mh.frameworkTrace(cid, mid, "exit");
	}
	
	public int getTotal() {
		String mid = "getTotal";
		mh.frameworkTrace(cid, mid, "enter");
		mh.frameworkTrace(cid, mid, "exit");
		return total;
	}
	
	private CAS getEmptyCas() throws ResourceInitializationException {
		String mid = "getEmptyCas";
		mh.frameworkTrace(cid, mid, "enter");
		CAS cas = null;
		while(cas == null) {
			//	Use class level locking to serialize access to CasCreationUtils
			//  Only one thread at the time can create a CAS. UIMA uses lazy
			//  initialization approach which can cause NPE when two threads
			//  attempt to initialize a CAS. 
			synchronized( CasCreationUtils.class) {
				cas = CasCreationUtils.createCas(cr_tsd, cr_tp, cr_fid, cr_properties);
			}
		}
		mh.frameworkTrace(cid, mid, "exit");
		return cas;
	}
	
	private CAS getNewCas() throws ResourceInitializationException, CollectionException, IOException {
		String mid = "getNewCas";
		mh.frameworkTrace(cid, mid, "enter");
		CAS cas = getEmptyCas();
		cr.getNext(cas);
		mh.frameworkTrace(cid, mid, "exit");
		return cas;
	}
	
	public CAS getUsedCas(CAS cas) throws CollectionException, IOException, ResourceInitializationException {
		String mid = "getUsedCas";
		mh.frameworkTrace(cid, mid, "enter");
		cas.reset();
		cr.getNext(cas);
		mh.frameworkTrace(cid, mid, "exit");
		return cas;
	}
	
	public CAS getCas(CAS cas) throws CollectionException, IOException, ResourceInitializationException {
		if(cas == null) {
			return getNewCas();
		}
		else {
			return getUsedCas(cas);
		}
	}
	
	public boolean hasNext() throws CollectionException, IOException {
		return cr.hasNext();
	}
	
}
