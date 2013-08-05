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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_component.JCasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.ducc.Workitem;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;
import org.apache.uima.util.XMLSerializer;
import org.xml.sax.SAXException;

public class DuccTextCC extends JCasAnnotator_ImplBase {
  private Logger logger;
  private String outputFilename=null;
  private File outFile;
  private FileOutputStream fos;
  private ZipOutputStream zos;


  public void initialize(UimaContext aContext) throws ResourceInitializationException {
    super.initialize(aContext);
    logger = aContext.getLogger();
  }

  public void process(JCas jcas) throws AnalysisEngineProcessException {
    Iterator<FeatureStructure> fsit = jcas.getIndexRepository().getAllIndexedFS(jcas.getCasType(Workitem.type));
    if (fsit.hasNext()) {
      Workitem wi = (Workitem) fsit.next();
      if (!outputFilename.equals(wi.getOutputspec())) {
    	  throw new AnalysisEngineProcessException(new RuntimeException("flush mismatch: "+outputFilename+" != "+wi.getOutputspec()));
      }
      try {
		zos.close();
		fos.close();
		if (!outFile.renameTo(new File(outputFilename))) {
			throw new IOException("Rename failed for "+outputFilename);
		}
	} catch (IOException e) {
		throw new AnalysisEngineProcessException(e);
	}
      logger.log(Level.INFO, "DuccDummyCC: Flushed "+wi.getOutputspec());
      return;
    }

    fsit = jcas.getIndexRepository().getAllIndexedFS(jcas.getCasType(DuccDocumentInfo.type));
    if (!fsit.hasNext()) {
      throw new AnalysisEngineProcessException(new RuntimeException("No DuccDocumentInfo FS in CAS"));
    }
    DuccDocumentInfo di = (DuccDocumentInfo) fsit.next();
//    logger.log(Level.FINE, "DuccDummyCC: No workitem FS found in CAS");
    String outputfile = di.getOutputfile();
    if (!outputfile.equals(outputFilename)) {
    	// create new output file
    	outputFilename = outputfile;
    	try {
        	outFile = new File(outputFilename+"_temp");
        	File outDir = outFile.getParentFile();
        	if (outDir != null && !outDir.exists()) {
        		outDir.mkdirs();
        	}
			fos = new FileOutputStream(outFile);
			zos = new ZipOutputStream(fos);
			zos.setLevel(7); //TODO turn off compression for binary
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    ZipEntry ze = new ZipEntry("doc_"+di.getDocseq());
    ze.setMethod(ZipEntry.DEFLATED);
    try {
		zos.putNextEntry(ze);
	    // write XMI
		XmiCasSerializer ser = new XmiCasSerializer(jcas.getTypeSystem());
	    XMLSerializer xmlSer = new XMLSerializer(zos, false);
		ser.serialize(jcas.getCas(), xmlSer.getContentHandler());
	    zos.closeEntry();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (SAXException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}

  }

}
