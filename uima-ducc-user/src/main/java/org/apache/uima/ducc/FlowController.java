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
import org.apache.uima.flow.CasFlowController_ImplBase;
import org.apache.uima.flow.CasFlow_ImplBase;
import org.apache.uima.flow.FinalStep;
import org.apache.uima.flow.Flow;
import org.apache.uima.flow.FlowControllerContext;
import org.apache.uima.flow.SimpleStep;
import org.apache.uima.flow.Step;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.OperationalProperties;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.analysis_engine.annotator.AnnotatorInitializationException;

/**
 * Ducc FlowController for Job Processes assembled from user components
 * If CM delegate exists then WI-Cas is first sent there
 *    and then optionally to CC delegate if so specified by flag in WorkItem feature structure.
 * If no CM delegate, then WI-Cas is sent to AE and CC if it exists.
 */
public class FlowController extends CasFlowController_ImplBase {

  private List<String> mSequence;
  private boolean mStartsWithCasMultiplier=false;
  private Type mWorkitemType;
  private Feature mSendToAllFeature;
  private Feature mSendToLastFeature;
  

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


  public void typeSystemInit(TypeSystem aTypeSystem) throws AnalysisEngineProcessException {
	  // Get a reference to the "Workitem" Type
	  mWorkitemType = aTypeSystem.getType("org.apache.uima.ducc.Workitem");
	  if (mWorkitemType == null) {
		  throw new AnalysisEngineProcessException(AnnotatorInitializationException.TYPE_NOT_FOUND,
				  new Object[] { getClass().getName(), "org.apache.uima.ducc.Workitem" });
	  }
	
	  // Get a reference to the "sendToALL" Feature
	  mSendToAllFeature = mWorkitemType.getFeatureByBaseName("sendToAll");
	  if (mSendToAllFeature == null) {
		  throw new AnalysisEngineProcessException(AnnotatorInitializationException.FEATURE_NOT_FOUND,
				  new Object[] { getClass().getName(), "org.apache.uima.ducc.Workitem:sendToAll" });
	  }
	
	  // Get a reference to the "sendToLast" Feature
	  mSendToLastFeature = mWorkitemType.getFeatureByBaseName("sendToLast");
	  if (mSendToLastFeature == null) {
		  throw new AnalysisEngineProcessException(AnnotatorInitializationException.FEATURE_NOT_FOUND,
				  new Object[] { getClass().getName(), "org.apache.uima.ducc.Workitem:sendToLast" });
	  }
  }
	  
  public Flow computeFlow(CAS aCAS) throws AnalysisEngineProcessException {
    return new FixedFlowObject(0);
  }

  class FixedFlowObject extends CasFlow_ImplBase {
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

    public Step next() throws AnalysisEngineProcessException {

      // If this is a work item CAS in a pipeline with an initial CM that has just been
      // to the CM then check if it should be sent to the last step, e.g. the CC.
      if (mStartsWithCasMultiplier && !internallyCreatedCas && currentStep == 1) {
        // Parent CAS has been to the initial CM, so see if a special flow has been requested.
        // Get an iterator only if the Workitem type is in the CAS's typesystem 
        // (avoids JCAS_TYPE_NOT_IN_CAS error)
 
    	FSIterator<FeatureStructure> it = this.getCas().getIndexRepository().getAllIndexedFS(mWorkitemType);
        if (it.isValid()) {
          FeatureStructure wi = it.get();
          it.moveToNext();
          if (it.isValid()) {
            throw new IllegalStateException("More than one instance of Workitem type");
          }
          if (wi.getBooleanValue(mSendToAllFeature)) {
        	// send WI-CAS to all delegates 
          }
          else if (wi.getBooleanValue(mSendToLastFeature)) {
        	// send to last delegate only
        	currentStep = mSequence.size() - 1;
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

    public Flow newCasProduced(CAS newCas, String producedBy) throws AnalysisEngineProcessException {
      // start the new output CAS from the next node after the CasMultiplier that produced it
      // (there may be a CM in other than the first step)
      int i = 0;
      while (!mSequence.get(i).equals(producedBy))
        i++;
      return new FixedFlowObject(i + 1, true);
    }
  }
}
