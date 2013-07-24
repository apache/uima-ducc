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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.analysis_engine.metadata.AnalysisEngineMetaData;
import org.apache.uima.analysis_engine.metadata.FixedFlow;
import org.apache.uima.analysis_engine.metadata.FlowConstraints;
import org.apache.uima.cas.CAS;
import org.apache.uima.ducc.Workitem;
import org.apache.uima.flow.FinalStep;
import org.apache.uima.flow.Flow;
import org.apache.uima.flow.FlowControllerContext;
import org.apache.uima.flow.JCasFlowController_ImplBase;
import org.apache.uima.flow.JCasFlow_ImplBase;
import org.apache.uima.flow.SimpleStep;
import org.apache.uima.flow.Step;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.cas.TOP;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.OperationalProperties;

/**
 * Ducc FlowController for Job Processes assembled from user components
 * If CM delegate exists then WI-Cas is first sent there
 *    and then optionally to CC delegate if so specified by flag in WorkItem feature structure.
 * If no CM delegate, then WI-Cas is sent to AE and CC if it exists.
 */
public class DuccJobProcessFC extends JCasFlowController_ImplBase {

  //private String cmKey="DuccXmiZipReaderCM";
  
  private List<String> mSequence;
  private Boolean mHasCasMultiplier=false;

  public void initialize(FlowControllerContext aContext) throws ResourceInitializationException {
    super.initialize(aContext);

    FlowConstraints flowConstraints = aContext.getAggregateMetadata().getFlowConstraints();
    mSequence = new ArrayList<String>();
    if (flowConstraints instanceof FixedFlow) {
      String[] sequence = ((FixedFlow) flowConstraints).getFixedFlow();
		  mSequence.addAll(Arrays.asList(sequence));
    } else {
      throw new ResourceInitializationException(ResourceInitializationException.FLOW_CONTROLLER_REQUIRES_FLOW_CONSTRAINTS,
              new Object[]{this.getClass().getName(), "fixedFlow", aContext.getAggregateMetadata().getSourceUrlString()});
    }

    // check if first delegate is a CasMultiplier
    Iterator aeIter = getContext().getAnalysisEngineMetaDataMap().entrySet().iterator();
    while (aeIter.hasNext()) {
      Map.Entry entry = (Map.Entry) aeIter.next();
      AnalysisEngineMetaData md = (AnalysisEngineMetaData) entry.getValue();
      OperationalProperties op = md.getOperationalProperties();
      if (op.getOutputsNewCASes()) {
    	mHasCasMultiplier = true;
      }
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.uima.flow.CasFlowController_ImplBase#computeFlow(org.apache.uima.cas.CAS)
   */
  public Flow computeFlow(JCas aCAS) throws AnalysisEngineProcessException {
    return new FixedFlowObject(0);
  }

  class FixedFlowObject extends JCasFlow_ImplBase {
    private int currentStep;
    private boolean hasBeenToCM = false;
    private boolean internallyCreatedCas = false;
    private boolean hasBeenToCC = false;

    /**
     * Create a new fixed flow starting at step <code>startStep</code> of the fixed sequence.
     * 
     * @param startStep
     *          index of mSequence to start at
     */
    public FixedFlowObject(int startStep) {
      this(startStep, false);
    }

    /**
     * Create a new fixed flow starting at step <code>startStep</code> of the fixed sequence.
     * 
     * @param startStep
     *          index of mSequence to start at
     * @param internallyCreatedCas
     *          true to indicate that this Flow object is for a CAS that was produced by a
     *          CasMultiplier within this aggregate.
     * 
     */
    public FixedFlowObject(int startStep, boolean internallyCreatedCas) {
      currentStep = startStep;
      this.internallyCreatedCas = internallyCreatedCas;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.uima.flow.Flow#next()
     */
    public Step next() throws AnalysisEngineProcessException {
      if (!internallyCreatedCas) {
    	// this is a Work Item CAS
        if (mHasCasMultiplier) {
          if (!hasBeenToCM) {
        	// will send it there this time thru
        	hasBeenToCM = true;
          }
          else {
        	// has been to CM, see if should go to CC
        	if (!hasBeenToCC) {
	          Iterator<TOP> fsIter = this.getJCas().getJFSIndexRepository().getAllIndexedFS(Workitem.type);
	          if (fsIter.hasNext()) {
	        	Workitem wi = (Workitem) fsIter.next();
	        	if (fsIter.hasNext()) {
	        	  throw new IllegalStateException("More than one instance of Workitem type");
	        	}
	        	if (wi.getSendToCC()) {
	        	  hasBeenToCC = true;
	        	  return new SimpleStep((String)mSequence.get(mSequence.size()-1));
	        	}
	        	else {
	        	  return new FinalStep();
	        	}
	          }
            }
          }
        }
      }

      if (hasBeenToCC || currentStep >= mSequence.size()) {
        return new FinalStep(); // this CAS is cooked
      }

      // send the CAS to the next AE in sequence.
      return new SimpleStep((String)mSequence.get(currentStep++));
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.uima.flow.CasFlow_ImplBase#newCasProduced(CAS, String)
     */
    public Flow newCasProduced(CAS newCas, String producedBy) throws AnalysisEngineProcessException {
      // since the CM will always be in position 0 ...
      return new FixedFlowObject(1, true);
    }
  }
}
