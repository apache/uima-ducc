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
package org.apache.uima.ducc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.analysis_engine.metadata.AnalysisEngineMetaData;
import org.apache.uima.analysis_engine.metadata.FixedFlow;
import org.apache.uima.analysis_engine.metadata.FlowConstraints;
import org.apache.uima.jcas.cas.TOP;
import org.apache.uima.ducc.Workitem;
import org.apache.uima.flow.FinalStep;
import org.apache.uima.flow.Flow;
import org.apache.uima.flow.FlowControllerContext;
import org.apache.uima.flow.JCasFlowController_ImplBase;
import org.apache.uima.flow.JCasFlow_ImplBase;
import org.apache.uima.flow.SimpleStep;
import org.apache.uima.flow.Step;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.OperationalProperties;

/**
 * Ducc FlowController for Job Processes assembled from user components
 * If CM delegate exists then WI-Cas is first sent there
 *    and then optionally to CC delegate if so specified by flag in WorkItem feature structure.
 * If no CM delegate, then WI-Cas is sent to AE and CC if it exists.
 */
public class FlowController extends JCasFlowController_ImplBase {

  private List<String> mSequence;
  private boolean mStartsWithCasMultiplier=false;

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

    // Check if first delegate is a CasMultiplier.
    // Any other CMs will have no special treatment, 
    // i.e. parent will follow children thru the rest of the pipeline
    
    Iterator<Entry<String, AnalysisEngineMetaData>> aeIter = getContext().getAnalysisEngineMetaDataMap().entrySet().iterator();
    while (aeIter.hasNext()) {
      Entry<String, AnalysisEngineMetaData> entry = aeIter.next();
      AnalysisEngineMetaData md = entry.getValue();
      OperationalProperties op = md.getOperationalProperties();
      if (op.getOutputsNewCASes()) {
        if (mSequence.get(0).equals(entry.getKey())) {
          mStartsWithCasMultiplier = true;
        }
      } 
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.uima.flow.JCasFlowController_ImplBase#computeFlow(org.apache.uima.cas.JCas)
   */
  public Flow computeFlow(JCas aCAS) throws AnalysisEngineProcessException {
    return new FixedFlowObject(0);
  }

  class FixedFlowObject extends JCasFlow_ImplBase {
    private int currentStep;
    private boolean internallyCreatedCas = false;

    /**
     * Create a new fixed flow starting at step <code>startStep</code> of the fixed sequence.
     * 
     * @param startStep
     *          index of mSequence to start at
     */
    private FixedFlowObject(int startStep) {
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
    private FixedFlowObject(int startStep, boolean internallyCreatedCas) {
      currentStep = startStep;
      this.internallyCreatedCas = internallyCreatedCas;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.uima.flow.Flow#next()
     */
    @Override
    public Step next() throws AnalysisEngineProcessException {

      // If this is a work item CAS in a pipeline with an initial CM that has just been
      // to the CM then check if it should be sent to the last step, e.g. the CC.
      if (mStartsWithCasMultiplier && !internallyCreatedCas && currentStep == 1) {
        // Parent CAS has been to the initial CM, so see if a special flow has been requested.
        // Get an iterator only if the Workitem type is in the CAS's typesystem 
        // (avoids JCAS_TYPE_NOT_IN_CAS error)
        Iterator<TOP> fsIter = null;

        if (this.getJCas().getTypeSystem().getType(Workitem.class.getName()) != null) {
          fsIter = this.getJCas().getJFSIndexRepository().getAllIndexedFS(Workitem.type);
        }
        if (fsIter != null && fsIter.hasNext()) {
          Workitem wi = (Workitem) fsIter.next();
          if (fsIter.hasNext()) {
            throw new IllegalStateException("More than one instance of Workitem type");
          }
          if (wi.getSendToAll()) {
        	// send WI-CAS to any remaining delegates 
          }
          else if (wi.getSendToLast()) {
          	// send WI-CAS to last delegate, unless the only delegate is the initial CM
          	if (currentStep < (mSequence.size() - 1)) {
          	  currentStep = mSequence.size() - 1;
          	}
          }
          else {
        	// send WI-CAS back to JD
          	return new FinalStep();
          }
        }
        // No Workitem FS in CAS, WI-CAS is at end of flow
        else return new FinalStep();
      }

      if (currentStep >= mSequence.size()) {
        return new FinalStep(); // this CAS is cooked
      }

      // Send to next component in pipeline
      return new SimpleStep((String)mSequence.get(currentStep++));
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.uima.flow.JCasFlow_ImplBase#newCasProduced(JCas, String)
     */
    @Override
    public Flow newCasProduced(JCas newCas, String producedBy) throws AnalysisEngineProcessException {
      // start the new output CAS from the next node after the CasMultiplier that produced it
      // (there may be a CM in other than the first step)
      int i = 0;
      while (!mSequence.get(i).equals(producedBy))
        i++;
      return new FixedFlowObject(i + 1, true);
    }
  }
}
