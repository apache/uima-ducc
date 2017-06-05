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
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.ducc.user.common.QuotedOptions;
import org.apache.uima.ducc.user.common.UimaUtils;
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
	
	protected AllInOne.MsgHandler mh;
	protected Properties properties = new Properties();

	private CollectionReader cr;
	private TypePriorities cr_tp;
	private TypeSystemDescription cr_tsd;
	private FsIndexDescription[] cr_fid;
	private Properties cr_properties = new Properties();
	
	private int total;
	
	public CasGenerator(Properties properties, AllInOne.MsgHandler mh) {
		if(properties != null) {
			this.properties = properties;
		}
		if(mh != null) {
			this.mh = mh;
		}
	}
	
	public void initialize() throws InvalidXMLException, ResourceConfigurationException, ResourceInitializationException {
		String mid = "initialize";
		String crDescriptor = properties.getProperty(AllInOne.DriverDescriptorCR);
		String crOverrides = properties.getProperty(AllInOne.DriverDescriptorCROverrides);
		XMLParser xmlParser = UIMAFramework.getXMLParser();
		XMLInputSource in = UimaUtils.getXMLInputSource(crDescriptor);
		ResourceSpecifier crrs = xmlParser.parseCollectionReaderDescription(in);
		// CR overrides - throw up if trying to override an undefined parameter
        ResourceCreationSpecifier specifier = (ResourceCreationSpecifier) crrs;
        ConfigurationParameterDeclarations configurationParameterDeclarations = specifier.getMetaData().getConfigurationParameterDeclarations();
        ConfigurationParameterSettings cps = specifier.getMetaData().getConfigurationParameterSettings();
        if(crOverrides != null) {
            // Tokenize override assignments on whitespace, honoring but stripping quotes
            // Then create a map from all of them
            ArrayList<String> toks = QuotedOptions.tokenizeList(crOverrides, true);
            Map<String,String> map = QuotedOptions.parseAssignments(toks, 0);
            for (Entry<String, String> ent : map.entrySet()) {
                String name = ent.getKey();
                String value = ent.getValue();
                String message = "config param: "+name+" = '"+value+"'";
                mh.frameworkDebug(cid, mid, message);
                ConfigurationParameter configurationParameter = UimaUtils.findConfigurationParameter(configurationParameterDeclarations, name);
                if (configurationParameter == null) {
                    throw new ResourceConfigurationException(
                                    ResourceConfigurationException.NONEXISTENT_PARAMETER, new Object[] { name, "CollectionReader" });
                }
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
	}
	
	public Progress[] getProgressArray() {
		Progress[] retVal;
		synchronized(cr) {
			retVal = cr.getProgress();
		}
		return retVal;
	}
	
	public Progress getProgress() {
		Progress progress = null;
		Progress[] progressArray = getProgressArray();
		if(progressArray != null) {
			progress = progressArray[0];
		}
		return progress;
	}
	
	private void initTotal() {
		Progress progress = getProgress();
		if(progress != null) {
			total = (int)progress.getTotal();
		}
	}
	
	public int getTotal() {
		return total;
	}
	
	private CAS getEmptyCas() throws ResourceInitializationException {
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
		return cas;
	}
	
	private CAS getNewCas() throws ResourceInitializationException, CollectionException, IOException {
		CAS cas = getEmptyCas();
		cr.getNext(cas);
		return cas;
	}
	
	public CAS getUsedCas(CAS cas) throws CollectionException, IOException, ResourceInitializationException {
		cas.reset();
		cr.getNext(cas);
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
