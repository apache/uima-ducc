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
package org.apache.uima.ducc.user.jd;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiSerializationSharedData;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.ducc.user.common.DuccUimaSerializer;
import org.apache.uima.ducc.user.common.ExceptionHelper;
import org.apache.uima.ducc.user.common.QuotedOptions;
import org.apache.uima.ducc.user.common.UimaUtils;
import org.apache.uima.resource.ResourceConfigurationException;
import org.apache.uima.resource.ResourceCreationSpecifier;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.ConfigurationParameter;
import org.apache.uima.resource.metadata.ConfigurationParameterDeclarations;
import org.apache.uima.resource.metadata.ConfigurationParameterSettings;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.Progress;
import org.apache.uima.util.XMLInputSource;
import org.apache.uima.util.XMLParser;
import org.xml.sax.SAXException;

public class JdUserCollectionReader {

	private static DuccUimaSerializer uimaSerializer = new DuccUimaSerializer();
	private static XmiSerializationSharedData xmiSerializationSharedData = new XmiSerializationSharedData();
	
	private String crXml = null;
	private String crCfg = null;

	private JdUserCasManager cm = null;
	private CollectionReader cr = null;

	private HashMap<String, String> overrides = new HashMap<String, String>();

	private int total = -1;
	
	private AtomicInteger seqNo = new AtomicInteger(0);
	
	public JdUserCollectionReader(String crXml, String crCfg) throws Exception {
		initialize(crXml, crCfg);
	}

	public void initialize(String crXml, String crCfg) throws Exception {
		try {
			_initialize(crXml, crCfg);
		}
		catch(Exception e) {
			Exception jdUserException = ExceptionHelper.wrapStringifiedException(e);
			throw jdUserException;
		}
	}
	
	private void _initialize(String crXml, String crCfg) throws InvalidXMLException, ResourceConfigurationException, ResourceInitializationException {
		setCrXml(crXml);
		setCrCfg(crCfg);
		//
		// Read and parse the collection reader
		//
		XMLParser xmlParser = UIMAFramework.getXMLParser();
		XMLInputSource in = null;
		String crDescriptor = crXml;
		in = UimaUtils.getXMLInputSource(crDescriptor);
		ResourceSpecifier crrs = xmlParser.parseCollectionReaderDescription(in);
		// CR overrides
		ResourceCreationSpecifier specifier = (ResourceCreationSpecifier) crrs;
		ConfigurationParameterDeclarations configurationParameterDeclarations = specifier
				.getMetaData().getConfigurationParameterDeclarations();
		ConfigurationParameterSettings cps = specifier.getMetaData()
				.getConfigurationParameterSettings();
		if (crCfg != null) {
			// Tokenize override assignments on whitespace, honoring but
			// stripping quotes
			// Then create a map from all of them
			ArrayList<String> toks = QuotedOptions.tokenizeList(crCfg, true);
			Map<String, String> map = QuotedOptions.parseAssignments(toks,
					false);
			for (Entry<String, String> ent : map.entrySet()) {
				String name = ent.getKey();
				String value = ent.getValue();
				overrides.put(name, value);
				ConfigurationParameter configurationParameter = UimaUtils
						.findConfigurationParameter(
								configurationParameterDeclarations, name);
				if (configurationParameter == null) {
					throw new ResourceConfigurationException(
							ResourceConfigurationException.NONEXISTENT_PARAMETER,
							new Object[] { name, "CollectionReader" });
				}
				Object object = UimaUtils.getOverrideValueObject(
						configurationParameter, value);
				cps.setParameterValue(name, object);
			}
		}
		// CollectionReader
		setCr(UIMAFramework.produceCollectionReader(crrs));
		initTotal();
		// CasManager
		cm = new JdUserCasManager(cr);
	}

	public int getTotal() {
		return total;
	}
	
	public JdUserMetaCas getJdUserMetaCas() throws Exception {
		try {
			return _getJdUserMetaCas();
		}
		catch(Exception e) {
			Exception jdUserException = ExceptionHelper.wrapStringifiedException(e);
			throw jdUserException;
		}
	}
	
	private JdUserMetaCas _getJdUserMetaCas() throws CollectionException, IOException, Exception {
		JdUserMetaCas retVal = null;
		cm.getEmptyCas();
		synchronized(cr) {
			if(cr.hasNext()) {
				CAS cas = cm.getEmptyCas();
				cr.getNext(cas);
				String serializedCas = serialize(cas);
				String documentText = CasHelper.getId(cas);
				retVal = new JdUserMetaCas(seqNo.incrementAndGet(), serializedCas, documentText);
				cm.recycle(cas);
			}
		}
		return retVal;
	}
	
	public String serialize(CAS cas) throws Exception {
		String serializedCas = uimaSerializer.serializeCasToXmi(cas, xmiSerializationSharedData);
		return serializedCas;
	}
	
	public CAS deserialize(String serializedCas) throws ResourceInitializationException, FactoryConfigurationError, ParserConfigurationException, SAXException, IOException {
		CAS cas = cm.getEmptyCas();
		boolean lenient = true;
		int mergePoint = -1;
		uimaSerializer.deserializeCasFromXmi(serializedCas, cas, xmiSerializationSharedData, lenient, mergePoint);
		return cas;
	}
	
	public void recycle(CAS cas) {
		cm.recycle(cas);
	}
	
	private void setCrXml(String value) {
		crXml = value;
	}

	public String getCrXml() {
		return crXml;
	}
	
	private void setCrCfg(String value) {
		crCfg = value;
	}

	public String getCrCfg() {
		return crCfg;
	}
	
	private void setCr(CollectionReader value) {
		cr = value;
	}

	public CollectionReader getCr() {
		return cr;
	}


	public Progress[] getProgressArray() {
		synchronized (cr) {
			return cr.getProgress();
		}
	}

	public Progress getProgress() {
		Progress progress = null;
		Progress[] progressArray = getProgressArray();
		if (progressArray != null) {
			progress = progressArray[0];
		}
		return progress;
	}

	private void initTotal() {
		Progress progress = getProgress();
		if (progress != null) {
			setTotal((int) progress.getTotal());
		}
	}

	private void setTotal(int value) {
		total = value;
	}

}
