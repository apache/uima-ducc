package org.apache.uima.ducc.test.randomsleep;

import java.util.Random;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_component.CasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

/**
 * challngr@us.ibm.com
 * May, 2011
 */

public class NoSleepAE extends CasAnnotator_ImplBase 
{

    Random r;
    long tid = Thread.currentThread().getId();
    Logger logger;
    static boolean initComplete = false;

	@Override
	public void initialize(UimaContext uimaContext) throws ResourceInitializationException {
		super.initialize(uimaContext);
        logger = uimaContext.getLogger();
	}
	
	@Override
	public void process(CAS cas) throws AnalysisEngineProcessException {
		String data = cas.getSofaDataString();
        
        int           qid        = Integer.parseInt(data);
        logger.log(Level.INFO, "Executing task " + qid + ".");
	}

}
