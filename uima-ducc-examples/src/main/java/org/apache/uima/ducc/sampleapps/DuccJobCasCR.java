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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.uima.cas.CAS;
import org.apache.uima.cas.CASException;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReader_ImplBase;
import org.apache.uima.ducc.Workitem;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;

/**
 * A DUCC Job collection reader that reads text files from a directory in the filesystem.
 * It can be configured with the following parameters:
 * <ul>
 * <li><code>InputSpec</code> - path to directory containing input *.zip files</li>
 * <li><code>OutputDirectory</code> - path to directory for output files</li>
 * <li><code>IgnorePreviousOutput</code> (optional) - flag to ignore previous output files</li>
 * <li><code>SendToLast</code> (optional) - flag to route WorkItem CAS to last pipeline component. Only used for jobs with initial CM.</li>
 * <li><code>SendToAll</code> (optional) - flag to route WorkItem CAS to all pipeline components. Only used for jobs with initial CM.</li>
 * </ul>
 * 
 */
public class DuccJobCasCR extends CollectionReader_ImplBase {
  /**
   * Name of configuration parameter that must be set to the path of a directory containing input
   * files.
   */
  public static final String PARAM_INPUTSPEC = "InputSpec";

  /**
   * Name of configuration parameter that must be set to the path of the base directory 
   * where output files will be created.
   */
  public static final String PARAM_OUTPUTDIR = "OutputDirectory";

  /**
   * Name of configuration parameter that indicates if previous output should be ignored.
   */
  public static final String PARAM_IGNOREPREVIOUS = "IgnorePreviousOutput";

  /**
   * Flag to route WorkItem CAS to last pipeline component. Used to flush any output data.
   * This string is ignored if the Job has a CM component.
   */
  public static final String PARAM_SENDTOLAST = "SendToLast";

  /**
   * Flag to route WorkItem CAS to all pipeline components.
   * If the Job has no CM component the WI CAS is already sent to AE and CC.
   */
  public static final String PARAM_SENDTOALL = "SendToAll";
  
  class WorkItem {
    public WorkItem(String absolutePathIn, String absolutePathOut) {
      filename=absolutePathIn;
      outname=absolutePathOut;
    }
    String filename;
    String outname;
  }

  private ArrayList<WorkItem> mWorkList; 

  private String mInputdirectory;

  private String mOutputdirectory;
  
  private Boolean mIgnorePrevious;

  private int mCurrentIndex;

  private Boolean mSendToLast;

  private Boolean mSendToAll;

  private int mPreviouslyDone;

  private Logger logger;

  /**
   * @see org.apache.uima.collection.CollectionReader_ImplBase#initialize()
   */
  public void initialize() throws ResourceInitializationException {
	logger = getUimaContext().getLogger();
    String inputSpec = ((String) getConfigParameterValue(PARAM_INPUTSPEC)).trim();
    mOutputdirectory = ((String) getConfigParameterValue(PARAM_OUTPUTDIR)).trim();
    mIgnorePrevious = (Boolean) getConfigParameterValue(PARAM_IGNOREPREVIOUS);
    mSendToLast = (Boolean) getConfigParameterValue(PARAM_SENDTOLAST);
    mSendToAll = (Boolean) getConfigParameterValue(PARAM_SENDTOALL);

    if (null == mIgnorePrevious) {
    	mIgnorePrevious = Boolean.FALSE;
    }
    if (null == mSendToLast) {
    	mSendToLast = Boolean.FALSE;
    }
    if (null == mSendToAll) {
    	mSendToAll = Boolean.FALSE;
    }
    mCurrentIndex = 0;
    mPreviouslyDone = 0;

    mInputdirectory = inputSpec;
    // if input directory does not exist or is not a directory, throw exception
    File inDirectory = new File(mInputdirectory);
    if (!inDirectory.exists() || !inDirectory.isDirectory()) {
      throw new ResourceInitializationException(new RuntimeException( mInputdirectory+" is not a directory"));
    }

    // if output directory is a file throw exception
    File outDirectory = new File(mOutputdirectory);
    if (outDirectory.exists() && !outDirectory.isDirectory()) {
      throw new ResourceInitializationException(new RuntimeException("Specified output directory "+mOutputdirectory+" is a file"));
    }

    logger.log(Level.INFO, "Processing \".zip files in"+mInputdirectory);
    if (null != mIgnorePrevious && mIgnorePrevious) {
      logger.log(Level.INFO, "Overwriting previous outfiles");
    }

    // get list of files in the specified directory
    mWorkList = new ArrayList<WorkItem>();
    addFilesFromDir(inDirectory, "zip");
    if (0 < mPreviouslyDone) {
      logger.log(Level.INFO, "Preserving "+mPreviouslyDone+" output files in "+mOutputdirectory);
    }
    logger.log(Level.INFO, "Processing "+mWorkList.size()+" output files in "+mOutputdirectory);
  }
  
  /**
   * This method adds files in the input directory,
   * if the respective output file does not exist,
   * or if mIgnorePrevious = true.
   * 
   * @param dir
   */
  private void addFilesFromDir(File dir, String ext) {
    File[] files = dir.listFiles();
    for (int i = 0; i < files.length; i++) {
      if (!files[i].isDirectory()) {
        String outfilename = files[i].getAbsolutePath();
        if (!outfilename.endsWith("."+ext)) {
          continue;
        }
        outfilename = outfilename.substring(mInputdirectory.length());
        outfilename = mOutputdirectory+outfilename;
        File outFile = new File(outfilename);
        if (!mIgnorePrevious && outFile.exists()) {
        	mPreviouslyDone++;
        }
        if (mIgnorePrevious || !outFile.exists()) {
        	mWorkList.add(new WorkItem(files[i].getAbsolutePath(),outfilename));
        	logger.log(Level.FINE, "adding "+outfilename);
        }
      }
    }
  }

  /**
   * @see org.apache.uima.collection.CollectionReader#hasNext()
   */
  public boolean hasNext() {
    return mCurrentIndex < mWorkList.size();
  }

  /**
   * @see org.apache.uima.collection.CollectionReader#getNext(org.apache.uima.cas.CAS)
   */
  public void getNext(CAS aCAS) throws IOException, CollectionException {
    JCas jcas;
    try {
      jcas = aCAS.getJCas();
      Workitem wi = new Workitem(jcas);
      wi.setInputspec(mWorkList.get(mCurrentIndex).filename);
      wi.setOutputspec(mWorkList.get(mCurrentIndex).outname);
      wi.setSendToLast(mSendToLast);
      wi.setSendToAll(mSendToAll);
      wi.addToIndexes();
      logger.log(Level.INFO, "Sending "+wi.getInputspec());
      mCurrentIndex++;
      jcas.setDocumentText(wi.getInputspec());
    } catch (CASException e) {
      throw new CollectionException(e);
    }

    //create WorkItem info structure
  }

  /**
   * @see org.apache.uima.collection.base_cpm.BaseCollectionReader#close()
   */
  public void close() throws IOException {
  }

  /**
   * @see org.apache.uima.collection.base_cpm.BaseCollectionReader#getProgress()
   */
  public Progress[] getProgress() {
    return new Progress[] { new ProgressImpl(mCurrentIndex, mWorkList.size(), Progress.ENTITIES) };
  }

  /**
   * Gets the total number of documents that will be returned by this collection reader. This is not
   * part of the general collection reader interface.
   * 
   * @return the number of documents in the collection
   */
  public int getNumberOfDocuments() {
    return mWorkList.size();
  }

}
