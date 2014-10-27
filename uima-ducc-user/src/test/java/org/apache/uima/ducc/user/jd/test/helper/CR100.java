package org.apache.uima.ducc.user.jd.test.helper;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReader_ImplBase;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;

public class CR100 extends CollectionReader_ImplBase {

	private int casLimit = 100;
	private AtomicInteger casCounter = new AtomicInteger(0);
	
	@Override
	public void getNext(CAS aCAS) throws IOException, CollectionException {
		aCAS.reset();
		int item = casCounter.incrementAndGet();
		if(item <= casLimit) {
			aCAS.setSofaDataString(""+item, "text");
		}
	}

	@Override
	public boolean hasNext() throws IOException, CollectionException {
		boolean retVal = false;
		if(casCounter.get() < casLimit) {
			retVal = true;
		}
		return retVal;
	}

	@Override
	public Progress[] getProgress() {
		ProgressImpl[] retVal = new ProgressImpl[1];
		retVal[0] = new ProgressImpl(casCounter.get(), casLimit, "CASes");
		return retVal;
	}

	@Override
	public void close() throws IOException {
	}

}
