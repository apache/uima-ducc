package org.apache.uima.ducc.test.randomsleep;

import java.io.IOException;

import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReader_ImplBase;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;

public class NoSleepCR extends CollectionReader_ImplBase {
		
	private volatile Logger logger;
    int qcount = 0;
	@Override
	public void initialize() throws ResourceInitializationException {	
		super.initialize();

        logger = getLogger();
		logger.log(Level.INFO, "initialize");

		String strcount = ((String) getConfigParameterValue("count"));
        qcount = Integer.parseInt(strcount);
	}

    static int get_next_counter = 0;	
	@Override
	public synchronized void getNext(CAS cas) throws IOException, CollectionException 
    {
        if ( get_next_counter < qcount ) {
            cas.reset();
            cas.setSofaDataString(""+get_next_counter, "text");
            logger.log(Level.INFO, " ****** getNext " + get_next_counter++);
        }
	}

	@Override
	public void close() throws IOException {
		logger.log(Level.INFO, "close");
	}

	@Override
	public Progress[] getProgress() {
		ProgressImpl[] retVal = new ProgressImpl[1];
		retVal[0] = new ProgressImpl(get_next_counter, qcount, "WorkItems");
		return retVal;
	}

	@Override
	public boolean hasNext() throws IOException, CollectionException {
		return get_next_counter < qcount;
	}

}
