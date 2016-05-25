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
 * This sample Cas Consumer is designed to create an output zip file for each Work Item.
 * The CAS compression format is selectable as either ZIP compressed XmiCas or UIMA 
 * compressed binary form 6 format. When compressed binary is used, each zip file also 
 * contains the full UIMA Type System in ZIP compressed text.
 * CASes in UIMA compressed binary form 6 format have the same flexibility as an XmiCas 
 * in that they can be deserialized into a CAS with a different, but compatible Type System.
 * 
 * See more information in DUCC Book chapters on sample applications.
 * 
 */

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_component.JCasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.cas.impl.Serialization;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.ducc.Workitem;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.Level;
import org.apache.uima.util.Logger;
import org.apache.uima.util.TypeSystemUtil;
import org.apache.uima.util.XMLSerializer;

public class DuccCasCC extends JCasAnnotator_ImplBase {

  public static final String PARAM_XMICOMPRESSIONLEVEL = "XmiCompressionLevel";
  public static final String PARAM_USEBINARYCOMPRESSION = "UseBinaryCompression";

  private Logger logger;
  private String outputFilename=null;
  private File outFile;
  private FileOutputStream fos;
  private ZipOutputStream zos;
  private boolean useBinaryCas;
  private int zipCompLevel;
  private String casExt;


  public void initialize(UimaContext aContext) throws ResourceInitializationException {
    super.initialize(aContext);
    zipCompLevel = (Integer)getContext().getConfigParameterValue(PARAM_XMICOMPRESSIONLEVEL);
    useBinaryCas = (null == getContext().getConfigParameterValue(PARAM_USEBINARYCOMPRESSION)) ? Boolean.FALSE :
    	(Boolean) getContext().getConfigParameterValue(PARAM_USEBINARYCOMPRESSION);
    logger = aContext.getLogger();
    if (useBinaryCas) {
    	zipCompLevel = 0;
    	casExt = "cas";
    	logger.log(Level.INFO, "Outputting CASes in UIMA compressed binary form 6");
    }
    else {
    	casExt = "xmi";
    	logger.log(Level.INFO, "Outputting CASes in XmiCas format, zip compressed at level="+zipCompLevel);
    }
  }

  public void process(JCas jcas) throws AnalysisEngineProcessException {
    Iterator<FeatureStructure> fsit = jcas.getIndexRepository().getAllIndexedFS(jcas.getCasType(Workitem.type));
    if (fsit.hasNext()) {
      Workitem wi = (Workitem) fsit.next();
      if (outputFilename == null || !outputFilename.equals(wi.getOutputspec())) {
    	  // this Work Item contained no documents. Create empty output file.
    	  try {
    		outFile = new File(wi.getOutputspec());
          	File outDir = outFile.getParentFile();
          	if (outDir != null && !outDir.exists()) {
          		outDir.mkdirs();
          	}
          	zos = new ZipOutputStream(new FileOutputStream(outFile));
    		zos.close();
    		logger.log(Level.INFO, "DuccCasCC: Flushed empty "+wi.getOutputspec());
    		return;
		} catch (Exception e) {
        	throw new AnalysisEngineProcessException(e);
		}
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
      logger.log(Level.INFO, "DuccCasCC: Flushed "+wi.getOutputspec());
      return;
    }

    fsit = jcas.getIndexRepository().getAllIndexedFS(jcas.getCasType(DuccDocumentInfo.type));
    if (!fsit.hasNext()) {
      throw new AnalysisEngineProcessException(new RuntimeException("No DuccDocumentInfo FS in CAS"));
    }
    DuccDocumentInfo di = (DuccDocumentInfo) fsit.next();
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
			zos = new ZipOutputStream(new BufferedOutputStream(fos,1024*100));
		    if (useBinaryCas) {
		      //put the output CAS typesystem in the output zipfile
		      ZipEntry ze = new ZipEntry("typesystem.xml");
		      ze.setMethod(ZipEntry.DEFLATED);
		      zos.setLevel(9);
		      zos.putNextEntry(ze);
		      TypeSystem ts = jcas.getTypeSystem();
		      TypeSystemDescription tsDesc = TypeSystemUtil.typeSystem2TypeSystemDescription(ts);
		      tsDesc.toXML(zos); // Capture type system in XML format
		      zos.closeEntry();
		    }
			zos.setLevel(zipCompLevel);
        } catch (Exception e) {
        	throw new AnalysisEngineProcessException(e);
        }
    }

    ZipEntry ze = new ZipEntry("doc_"+di.getDocseq()+"."+casExt);
    ze.setMethod(ZipEntry.DEFLATED);
    try {
		zos.putNextEntry(ze);
		BufferedOutputStream bos = new BufferedOutputStream(zos,1024*10);
		if (useBinaryCas) {
		  Serialization.serializeWithCompression(jcas.getCas(), bos, jcas.getTypeSystem());
		}
		else {
			// write XMI
		  XmiCasSerializer ser = new XmiCasSerializer(jcas.getTypeSystem());
		  XMLSerializer xmlSer = new XMLSerializer(bos, false);
		  ser.serialize(jcas.getCas(), xmlSer.getContentHandler());
		}
		bos.flush();
	    zos.closeEntry();
	} catch (Exception e) {
	      throw new AnalysisEngineProcessException(e);
	}

  }

}
