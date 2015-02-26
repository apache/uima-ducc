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
package org.apache.uima.ducc.jd.client;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccLoggerComponents;
import org.apache.uima.ducc.common.utils.QuotedOptions;
import org.apache.uima.ducc.jd.IJobDriver;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
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
import org.apache.uima.util.CasCopier;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.Progress;
import org.apache.uima.util.XMLInputSource;
import org.apache.uima.util.XMLParser;


public class CasSource {

	private static DuccLogger duccOut = DuccLoggerComponents.getJdOut(CasSource.class.getName());
	
	private ConcurrentLinkedQueue<CAS> recycledCasList = new ConcurrentLinkedQueue<CAS>();
	private AtomicBoolean exhaustedReader = new AtomicBoolean(false);
	private AtomicInteger seqNo = new AtomicInteger(0);
	
	private CasLimbo casLimbo;
	
	private CasDispatchMap casDispatchMap;
	
	private CollectionReader cr;
	private TypePriorities tp;
	private TypeSystemDescription tsd;
	private FsIndexDescription[] fsid;
	private Properties properties;
	
	private int total = -1;
	
	public CasSource(IJobDriver jd, String crxml, String crcfg, CasDispatchMap casDispatchMap) 
		throws IOException, InvalidXMLException, ResourceInitializationException, ResourceConfigurationException {
		init(jd, crxml, crcfg, casDispatchMap);
	}
	
    private void init(IJobDriver jd, String crxml, String crcfg, CasDispatchMap casDispatchMap) throws IOException, ResourceInitializationException, InvalidXMLException, ResourceConfigurationException {
        String location = "init";
        this.casDispatchMap = casDispatchMap;
        casLimbo = new CasLimbo(jd);
        IDuccWorkJob job = jd.getJob();
        // CR descriptor
        String crDescriptor = crxml;
        //
        // Read and parse the collection reader
        //
        XMLParser xmlParser = UIMAFramework.getXMLParser();
        XMLInputSource in = null;
		try {
			in = UimaUtils.getXMLInputSource(crDescriptor);
		} catch (InvalidXMLException e) {
            duccOut.error(location, job.getDuccId(), e);
            throw e;
		}
        ResourceSpecifier crrs = xmlParser.parseCollectionReaderDescription(in);

        duccOut.debug(location, job.getDuccId(), crcfg);
        // CR overrides
        ResourceCreationSpecifier specifier = (ResourceCreationSpecifier) crrs;
        ConfigurationParameterDeclarations configurationParameterDeclarations = specifier.getMetaData().getConfigurationParameterDeclarations();
        ConfigurationParameterSettings cps = specifier.getMetaData().getConfigurationParameterSettings();
        if(crcfg!= null) {
            // Tokenize override assignments on whitespace, honoring but stripping quotes
            // Then create a map from all of them
            ArrayList<String> toks = QuotedOptions.tokenizeList(crcfg, true);
            Map<String,String> map = QuotedOptions.parseAssignments(toks, 0);
            for (Entry<String, String> ent : map.entrySet()) {
                String name = ent.getKey();
                String value = ent.getValue();
                duccOut.debug(location, job.getDuccId(), "config param name:"+name+" "+"value:"+value);
                ConfigurationParameter configurationParameter = UimaUtils.findConfigurationParameter(configurationParameterDeclarations, name);
                if (configurationParameter == null) {
                    throw new ResourceConfigurationException(
                                    ResourceConfigurationException.NONEXISTENT_PARAMETER, new Object[] { name, "CollectionReader" });
                }
                Object object = UimaUtils.getOverrideValueObject(configurationParameter, value);
                cps.setParameterValue(name, object);
            }
        }
        // CR
        duccOut.debug(location, job.getDuccId(), "CR creation...");
        cr = UIMAFramework.produceCollectionReader(crrs);
        Properties props = new Properties();
        //  Change the heap size to reduce CAS size. The size here depends on what we intend to 
        //  put into the CAS. If its just a pointer to data (like question id), the size of the
        //  heap can be reduced even more
        props.setProperty(UIMAFramework.CAS_INITIAL_HEAP_SIZE, "1000");
        //  save CR type system. It will be used to initialize CASes later, in getEmptyCas()
        init(cr.getProcessingResourceMetaData().getTypeSystem(),
             cr.getProcessingResourceMetaData().getTypePriorities(),
             cr.getProcessingResourceMetaData().getFsIndexes(),
             props);
        duccOut.debug(location, job.getDuccId(), "CR created.");
    }
	
    public void init(TypeSystemDescription tsd,TypePriorities tp,FsIndexDescription[] fsid,Properties properties) {
    	this.tsd = tsd;
    	this.tp = tp;
    	this.fsid = fsid;
    	this.properties = properties;
    	initTotal();
    }
	
    private CAS clone(CAS casOriginal, int seqNo) throws ResourceInitializationException {
    	String location = "clone";
		CAS casClone = getEmptyCas(seqNo);
		CasCopier.copyCas(casOriginal, casClone, true);
		duccOut.debug(location, null, "seqNo:"+seqNo+" "+"casId:"+casOriginal.hashCode()+" "+"casId:"+casClone.hashCode());
		return casClone;
    }
    
	private CAS getEmptyCas(int seqNo) throws ResourceInitializationException {
		String location = "getEmptyCas";
		CAS cas = getRecycledCas();
		String type = "reuse";
		while(cas == null) {
			//	Use class level locking to serialize access to CasCreationUtils
			//  Only one thread at the time can create a CAS. UIMA uses lazy
			//  initialization approach which can cause NPE when two threads
			//  attempt to initialize a CAS. 
			synchronized( CasCreationUtils.class) {
				cas = CasCreationUtils.createCas(tsd, tp, fsid, properties);
			}
			type = "new";
			if(!casDispatchMap.reserveKey(cas)) {
				type = "duplicate";
				duccOut.debug(location, null, "type:"+type+" "+"seqNo:"+seqNo+" "+"casId:"+cas.hashCode());
				cas = null;
			}
		}
		duccOut.debug(location, null, "type:"+type+" "+"seqNo:"+seqNo+" "+"casId:"+cas.hashCode());
		return cas;
	}
	
	private CAS getRecycledCas() {
		CAS cas = null;
		if(!recycledCasList.isEmpty()) {
			cas = recycledCasList.poll();
		}
		return cas;
	}
	
	
	private void putRecycledCas(CAS cas) {
		assert(cas != null);
		recycledCasList.add(cas);
	}

	boolean recycleDisabled = false;
	
	public void recycle(CAS cas) {
		assert(cas != null);
		if(recycleDisabled) {
			return;
		}
		cas.reset();
		putRecycledCas(cas);
	}
	
	public void push(CasTuple casTuple) {
		String location = "push";
		assert(casTuple != null);
		casLimbo.put(casTuple);
		duccOut.debug(location, null, "seqNo:"+casTuple.getSeqno()+" "+"casId:"+casTuple.getCas().hashCode()+" "+"size:"+casLimbo.size());
	}
	
	public boolean isExhaustedReader() {
		String location = "isExhaustedReader";
		boolean retVal = exhaustedReader.get();
		duccOut.debug(location, null, retVal);
		return retVal;
	}
	
	public int getLimboSize() {
		return casLimbo.size();
	}
	
	public boolean isLimboEmpty() {
		return casLimbo.isEmpty();
	}
	
	public boolean hasLimboAvailable() {
		return casLimbo.hasAvailable();
	}
	
	public ArrayList<CasTuple> releaseLimbo() {
		return casLimbo.release();
	}
	
	public boolean hasDelayed() {
		return casLimbo.delayedSize() > 0;
	}
	
	public boolean isEmpty() {
		boolean retVal = false;
		if(isExhaustedReader()) {
			if(isLimboEmpty()) {
				retVal = true;
			}
		}
		return retVal;
	}
	
	public CasTuple pop() throws Exception {
		String location = "pop";
		CasTuple casTuple = casLimbo.get();
		if(casTuple != null) {
			CAS casOriginal = casTuple.getCas();
			int seqNo = casTuple.getSeqno();
			CAS casClone = clone(casOriginal, seqNo);
			casTuple.setCas(casClone);
		}
		else {
			try {
				synchronized(cr) {
					if((total > 0) && (total == seqNo.get())) {
						exhaustedReader.set(true);
					}
					else if(cr.hasNext()) {
						int next = seqNo.addAndGet(1);
						CAS cas = recycledCasList.poll();
						if(cas == null) {
							cas = getEmptyCas(next);
						}
							cr.getNext(cas);
							casTuple = new CasTuple(cas,next);
					}
					else {
						exhaustedReader.set(true);
					}
				}
			}
			catch (CollectionException e) {
				duccOut.error(location, null, e);
				throw e;
			} 
			catch (IOException e) {
				duccOut.error(location, null, e);
				throw e;
			} 
			catch (ResourceInitializationException e) {
				duccOut.error(location, null, e);
				throw e;
			}
		}
		if(casTuple != null) {
			duccOut.debug(location, null, "seqNo:"+casTuple.getSeqno()+" "+"casId:"+casTuple.getCas().hashCode()+" "+"size:"+casLimbo.size());
		}
		return casTuple;
	}
	
	public Progress[] getProgressArray() {
		synchronized(cr) {
			return cr.getProgress();
		}
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
	
	public int getSeqNo() {
		return seqNo.get();
	}
	
	public void rectifyStatus() {
		if(casLimbo != null) {
			casLimbo.rectifyStatus();
		}
	}
}
