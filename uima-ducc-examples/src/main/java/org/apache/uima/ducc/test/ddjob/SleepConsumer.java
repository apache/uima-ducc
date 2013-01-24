package org.apache.uima.ducc.test.ddjob;

import java.util.StringTokenizer;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_component.CasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.resource.ResourceInitializationException;

/**
 * challngr@us.ibm.com
 * May, 2011
 */

public class SleepConsumer extends CasAnnotator_ImplBase 
{
    String CC_Identifier = "*@@@@@@@@@ CC ";

	@Override
	public void initialize(UimaContext uimaContext) throws ResourceInitializationException 
    {
		super.initialize(uimaContext);
        System.out.println(CC_Identifier + "Consumer init called");

	}
	
    public void destroy()
    {
        System.out.println(CC_Identifier + " Destroy is called (0)");
        System.out.println(CC_Identifier + "Destroy exits");
    }

	@Override
	public void process(CAS cas) throws AnalysisEngineProcessException {
		String data = cas.getSofaDataString();

        StringTokenizer tok = new StringTokenizer(data);

        long          elapsed    = Long.parseLong(tok.nextToken());
        int           seqno        = Integer.parseInt(tok.nextToken());
        int           total      = Integer.parseInt(tok.nextToken());
        double        error_rate = Double.parseDouble(tok.nextToken());
        String        logid      = tok.nextToken();

        System.out.println(CC_Identifier + "next returns: " +
                           "Work Item(" + seqno + ") " +
                           "Sleep Time(" + elapsed + ") " +
                           "of total(" + total + ") " +
                           "error_rate(" + error_rate + ") " +
                           "logid(" + logid + ") " +
                           "pass to AE");

	}


}
