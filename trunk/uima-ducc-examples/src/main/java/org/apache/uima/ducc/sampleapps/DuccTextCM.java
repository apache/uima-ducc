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

package org.apache.uima.ducc.sampleapps;

/*
 * This sample Cas Multiplier uses paragraph boundaries to segment a text file, 
 * or a part of a text file, into multiple documents. A child CAS is created
 * for each document. Paragraphs that cross block boundaries are processed
 * in the block where they started. An error is thrown if a paragraph crosses 
 * two block boundaries.
 * 
 * See more information in DUCC Book chapters on sample applications.
 * 
 */

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_component.JCasMultiplier_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.AbstractCas;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.ducc.Workitem;
import org.apache.uima.ducc.sampleapps.DuccDocumentInfo;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;

public class DuccTextCM extends JCasMultiplier_ImplBase {
  private byte[] buffer = null;
  private int buffsize;
  private FileInputStream fis;
  private String inputFileName;
  private String outputFileName;
  private String language;
  private String encoding;
  private String nextDoc;
  private int nextDocOffset;
  private int bytelength;
  private int blockindex;
  private boolean newWI;
  private boolean spilled;
  private boolean firstdoc;
  private boolean lastblock;
  private int docInWI;
  private long filesize;
  private Workitem wi;
  private int currentindex;
  private Logger logger;
  FileChannel fc;

  private enum NextDoc { FIRSTDOC, SEP_IN_LASTBLOCK, NORMAL };
  private NextDoc strategy;
  
  private final int DEFAULT_BUFFER_SIZE = 20000000;

  public boolean hasNext() throws AnalysisEngineProcessException {
	if (spilled) {
	  return false;
	}
	try {
      return findnextdoc(strategy);
	} catch (IOException e) {
	  throw new AnalysisEngineProcessException(e);
	}
  }

  public AbstractCas next() throws AnalysisEngineProcessException {
    JCas newcas = getEmptyJCas();
    newcas.setDocumentText(getNextDocument());
    newcas.setDocumentLanguage(language);
    DuccDocumentInfo di = new DuccDocumentInfo(newcas);
    di.setInputfile(inputFileName);
    di.setOutputfile(outputFileName);
    di.setDocseq(docInWI++);
    di.setByteoffset(wi.getBlockindex() * wi.getBlocksize() + nextDocOffset);
    di.addToIndexes();
    return newcas;
  }

  @Override
  public void process(JCas jcas) throws AnalysisEngineProcessException {
    Iterator<FeatureStructure> fsit = jcas.getIndexRepository().getAllIndexedFS(jcas.getCasType(Workitem.type));
    if (!fsit.hasNext()) {
      throw new AnalysisEngineProcessException(new RuntimeException("No workitem FS in CAS"));
    }
    wi = (Workitem) fsit.next();
    logger.log(Level.INFO, "DuccTextCM: "+wi.getInputspec()+" at block "+wi.getBlockindex()+" length "+wi.getBytelength()+
    		" offset "+wi.getBlockindex() * wi.getBlocksize()+" outputs "+wi.getOutputspec());
    try {
      openInputFile(wi);
    } catch (IOException e) {
      throw new AnalysisEngineProcessException(e);
    }

    if (buffer == null) {
      if (wi.getBlocksize()>0) {
    	buffer = new byte[wi.getBlocksize() * 2];
    	buffsize = wi.getBlocksize() * 2;
      }
      else {
    	buffer = new byte[DEFAULT_BUFFER_SIZE];
    	buffsize = DEFAULT_BUFFER_SIZE;
      }
    }
    else {
      if (wi.getBytelength() > buffsize) {
    	buffer = new byte[wi.getBytelength() * 2];
        buffsize = wi.getBytelength();
      }
    }

    spilled = false;
    docInWI = 0;
    strategy = (blockindex == 0) ? NextDoc.FIRSTDOC : NextDoc.NORMAL;
  }


  public void initialize(UimaContext aContext) throws ResourceInitializationException {
    super.initialize(aContext);
    logger = aContext.getLogger();
  }


  private void openInputFile(Workitem wi) throws IOException {
    inputFileName = wi.getInputspec();
    outputFileName = wi.getOutputspec();
    bytelength = wi.getBytelength();
    blockindex = wi.getBlockindex();
    lastblock = wi.getLastBlock();
    language = wi.getLanguage();
    fis = new FileInputStream(new File(inputFileName));
    encoding = (null==wi.getEncoding()) ? "UTF-8" : wi.getEncoding();
    fc = fis.getChannel();
    long start = wi.getBlockindex() * wi.getBlocksize();
    filesize = fc.size();
    if (start > filesize) {
      throw new IOException("Specifid start position beyond end of input file "+inputFileName);
    }
    fis.skip(start);
	newWI = true;
  }

  private boolean findnextdoc(NextDoc condition) throws IOException {
    int startloc=-1;

    if (newWI) {
      newWI = false;
      int len = fis.read(buffer,0,bytelength);
      if (len != bytelength) {
    	throw new IOException("Read "+len+" bytes, expected "+bytelength);
      }
   	  currentindex = 0;
    }

    if (condition.equals(NextDoc.SEP_IN_LASTBLOCK)) {
    	// separator found at end of last block
    	if (10 == buffer[currentindex] && 10 == buffer[currentindex+1]) {
      	  return false;
      	}
      	if (10 == buffer[currentindex]) {
      	  currentindex++; // point at first char in Doc
      	}
      	startloc=currentindex;

        // find end of next doc
        int endloc=0;
        while (currentindex < (bytelength-1)) {
          if (10 == buffer[currentindex] && 10 == buffer[currentindex+1]) {
        	endloc = currentindex - 1;
        	break;
          }
          else {
        	currentindex++;
          }
        }
        if (endloc == 0) {
          throw new RuntimeException("Document larger than "+bytelength+" found in "+inputFileName+" block "+blockindex);
        }
        byte [] docbytes = Arrays.copyOfRange(buffer, startloc, endloc);
        nextDoc = new String(docbytes, encoding);
        nextDocOffset = startloc;
        return true;
      }

    if (condition.equals(NextDoc.FIRSTDOC)) {
      // special handling at beginning of first block
      // skip any leading EOL to find start of first doc
      // only execute this once
      strategy = NextDoc.NORMAL;
      while (10 == buffer[currentindex]) {
    	currentindex++;
    	if (currentindex == bytelength) {
    	  if (firstdoc) {
    		return false; // nothing but newlines in this block
    	  }
    	}
      }
    }

    if (condition.equals(NextDoc.NORMAL)) {
    	// currentindex either pointing at start of a segmentation, or 
    	// if a new block then possibly the middle of a previous document
      if (!(10 == buffer[currentindex] && 10 == buffer[currentindex+1])) {
      	// in the middle of a spilled Doc. Find next segmentation
      	while (currentindex < (bytelength-1)) {
      	  if (10 == buffer[currentindex] && 10 == buffer[currentindex+1]) {
      		break;
      	  }
      	  else {
      		currentindex++;
      	  }
      	}
      }
      if ( currentindex == bytelength-1) {
    	fis.close();
    	return false;
      }
      // now pointing at start of a segmentation, find start/end of next Doc
      while (10 == buffer[currentindex]) {
    	currentindex++;
    	if (currentindex == bytelength) {
    	  if (lastblock) {
    		fis.close();
    		return false;
    	  }
          // read next block and continue looking for end of Doc
    	  int len = fis.read(buffer,bytelength,bytelength);
    	  if (len <= 0) {
            throw new IOException("Read "+len+" bytes for "+inputFileName+" block "+blockindex+1);
    	  }
    	  fis.close();
    	  spilled = true;
    	  bytelength += len;
    	  return findnextdoc(NextDoc.SEP_IN_LASTBLOCK);
    	}
      }
    }

    startloc = currentindex;
    // find end of Doc
    int endloc=0;
    while (currentindex < (bytelength-1)) {
      if (10 == buffer[currentindex] && 10 == buffer[currentindex+1]) {
    	endloc = currentindex - 1;
      	break;
      }
      else {
    	currentindex++;
      }
    }

      if (endloc == 0) {
    	if (lastblock) {
    	  endloc = bytelength-1;
    	}
    	else {
    	  // read next block and continue looking for end of Doc
          int len = fis.read(buffer,bytelength,bytelength);
          if (len <= 0) {
        	throw new IOException("Read "+len+" bytes for "+inputFileName+" block "+blockindex+1);
          }
          fis.close();
          spilled = true;
          bytelength += len;
    	}
        while (currentindex < (bytelength-1)) {
          if (10 == buffer[currentindex] && 10 == buffer[currentindex+1]) {
        	endloc = currentindex - 1;
          	break;
          }
          else {
          	currentindex++;
          }
        }
        endloc = currentindex - 1;
      }
      byte [] docbytes = Arrays.copyOfRange(buffer, startloc, endloc);
      nextDoc = new String(docbytes, encoding);
      nextDocOffset = startloc;
      return true;
  }

  private String getNextDocument() {
    return nextDoc;
  }

}
