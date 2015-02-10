/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.uima.ducc.test.randomsleep;

import java.util.LinkedList;
import java.util.StringTokenizer;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_component.CasMultiplier_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.AbstractCas;
import org.apache.uima.cas.CAS;
import org.apache.uima.resource.ResourceInitializationException;

/**
 * An example CasMultiplier, which generates the specified number of output CASes,
 * used in the DD version of the system tests.
 */
public class SleepMultiplier extends CasMultiplier_ImplBase 
{
    String CM_Identifier = "*^^^^^^^^^ CM ";
    LinkedList<CAS> work = new LinkedList<CAS>();

    /*
     * (non-Javadoc)
     * 
     * @seeorg.apache.uima.analysis_component.AnalysisComponent_ImplBase#initialize(org.apache.uima.
     * UimaContext)
     */
    public void initialize(UimaContext aContext) throws ResourceInitializationException 
    {
        super.initialize(aContext);        
        System.out.println(CM_Identifier + "Multiplier init called");
    }

    public void destroy()
    {
        System.out.println(CM_Identifier + " Destroy is called (0)");
        System.out.println(CM_Identifier + "Destroy exits");
    }

    /*
     * (non-Javadoc)
     * 
     * @see JCasMultiplier_ImplBase#process(JCas)
     */
    public void process(CAS cas) throws AnalysisEngineProcessException 
    {
        String data = cas.getSofaDataString();
        System.out.println(CM_Identifier + "Multiplier process CAS. Data: " + data);

        work.add(cas);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.uima.analysis_component.AnalysisComponent#hasNext()
     */
    public boolean hasNext() throws AnalysisEngineProcessException 
    {
        System.out.println(CM_Identifier + " hasNext: true");
        return (work.size() > 0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.uima.analysis_component.AnalysisComponent#next()
     */
    public AbstractCas next() throws AnalysisEngineProcessException 
    {
        CAS nextCas = work.removeFirst();
        String data = nextCas.getSofaDataString();
        StringTokenizer tok = new StringTokenizer(data);

        long          elapsed    = Long.parseLong(tok.nextToken());
        int           seqno        = Integer.parseInt(tok.nextToken());
        int           total      = Integer.parseInt(tok.nextToken());
        String        logid      = tok.nextToken();

        System.out.println(CM_Identifier + "next returns: " +
                           "Work Item(" + seqno + ") " +
                           "Sleep Time(" + elapsed + ") " +
                           "of total(" + total + ") " +
                           "logid(" + logid + ") " +
                           "pass to AE");

        CAS cas = getEmptyCAS();
        cas.setSofaDataString(data, "text");

        return cas;
    }

}
