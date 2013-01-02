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
import java.util.List;

import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.analysis_engine.metadata.FixedFlow;
import org.apache.uima.analysis_engine.metadata.FlowConstraints;
import org.apache.uima.cas.CAS;
import org.apache.uima.flow.CasFlowController_ImplBase;
import org.apache.uima.flow.CasFlow_ImplBase;
import org.apache.uima.flow.FinalStep;
import org.apache.uima.flow.Flow;
import org.apache.uima.flow.FlowControllerContext;
import org.apache.uima.flow.SimpleStep;
import org.apache.uima.flow.Step;
import org.apache.uima.resource.ResourceInitializationException;

/**
 * Ducc FlowController for Job Processes assembled from user components
 * If WI-Cas is sent to initial CM, it is then dropped
 * All other parent CASes continue thru the flow
 */
public class DuccJobProcessFC extends CasFlowController_ImplBase {

  /**
   */
  private String cmKey="DuccXmiZipReaderCM";
  private List<String> mSequence;

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
  }

  /*
   * (non-Javadoc)
   * 
   * @see org.apache.uima.flow.CasFlowController_ImplBase#computeFlow(org.apache.uima.cas.CAS)
   */
  public Flow computeFlow(CAS aCAS) throws AnalysisEngineProcessException {
    return new FixedFlowObject(0);
  }

  class FixedFlowObject extends CasFlow_ImplBase {
    private int currentStep;
    private boolean hasBeenToDuccCM = false;
    private boolean internallyCreatedCas = false;

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
      // Drop the Work Item CAS after it has been to the Ducc CM
      if (!internallyCreatedCas) {
        if (hasBeenToDuccCM) {
          return new FinalStep();
        }
      }

      if (currentStep >= mSequence.size()) {
        return new FinalStep(); // this CAS has finished the sequence
      }

      // Check if WI CAS is going to Ducc CM
      if (!internallyCreatedCas && cmKey.equals(mSequence.get(currentStep))) {
        // set flag for next time thru
        hasBeenToDuccCM = true;
      }

      // now send the CAS to the next AE in sequence.
      return new SimpleStep((String)mSequence.get(currentStep++));
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.uima.flow.CasFlow_ImplBase#newCasProduced(CAS, String)
     */
    public Flow newCasProduced(CAS newCas, String producedBy) throws AnalysisEngineProcessException {
      // start the new output CAS from the next node after the CasMultiplier that produced it
      int i = 0;
      while (!mSequence.get(i).equals(producedBy))
        i++;
      return new FixedFlowObject(i + 1, true);
    }
  }
}
