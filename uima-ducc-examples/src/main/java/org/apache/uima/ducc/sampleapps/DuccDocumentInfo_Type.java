
/* First created by JCasGen Wed Jul 31 15:14:59 EDT 2013 */
package org.apache.uima.ducc.sampleapps;

import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.JCasRegistry;
import org.apache.uima.cas.impl.CASImpl;
import org.apache.uima.cas.impl.FSGenerator;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.cas.impl.TypeImpl;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.impl.FeatureImpl;
import org.apache.uima.cas.Feature;
import org.apache.uima.jcas.cas.TOP_Type;

/** 
 * Updated by JCasGen Thu Aug 01 14:48:37 EDT 2013
 * @generated */
public class DuccDocumentInfo_Type extends TOP_Type {
  /** @generated */
  protected FSGenerator getFSGenerator() {return fsGenerator;}
  /** @generated */
  private final FSGenerator fsGenerator = 
    new FSGenerator() {
      public FeatureStructure createFS(int addr, CASImpl cas) {
  			 if (DuccDocumentInfo_Type.this.useExistingInstance) {
  			   // Return eq fs instance if already created
  		     FeatureStructure fs = DuccDocumentInfo_Type.this.jcas.getJfsFromCaddr(addr);
  		     if (null == fs) {
  		       fs = new DuccDocumentInfo(addr, DuccDocumentInfo_Type.this);
  			   DuccDocumentInfo_Type.this.jcas.putJfsFromCaddr(addr, fs);
  			   return fs;
  		     }
  		     return fs;
        } else return new DuccDocumentInfo(addr, DuccDocumentInfo_Type.this);
  	  }
    };
  /** @generated */
  public final static int typeIndexID = DuccDocumentInfo.typeIndexID;
  /** @generated 
     @modifiable */
  public final static boolean featOkTst = JCasRegistry.getFeatOkTst("org.apache.uima.ducc.sampleapps.DuccDocumentInfo");
 
  /** @generated */
  final Feature casFeat_inputfile;
  /** @generated */
  final int     casFeatCode_inputfile;
  /** @generated */ 
  public String getInputfile(int addr) {
        if (featOkTst && casFeat_inputfile == null)
      jcas.throwFeatMissing("inputfile", "org.apache.uima.ducc.sampleapps.DuccDocumentInfo");
    return ll_cas.ll_getStringValue(addr, casFeatCode_inputfile);
  }
  /** @generated */    
  public void setInputfile(int addr, String v) {
        if (featOkTst && casFeat_inputfile == null)
      jcas.throwFeatMissing("inputfile", "org.apache.uima.ducc.sampleapps.DuccDocumentInfo");
    ll_cas.ll_setStringValue(addr, casFeatCode_inputfile, v);}
    
  
 
  /** @generated */
  final Feature casFeat_outputfile;
  /** @generated */
  final int     casFeatCode_outputfile;
  /** @generated */ 
  public String getOutputfile(int addr) {
        if (featOkTst && casFeat_outputfile == null)
      jcas.throwFeatMissing("outputfile", "org.apache.uima.ducc.sampleapps.DuccDocumentInfo");
    return ll_cas.ll_getStringValue(addr, casFeatCode_outputfile);
  }
  /** @generated */    
  public void setOutputfile(int addr, String v) {
        if (featOkTst && casFeat_outputfile == null)
      jcas.throwFeatMissing("outputfile", "org.apache.uima.ducc.sampleapps.DuccDocumentInfo");
    ll_cas.ll_setStringValue(addr, casFeatCode_outputfile, v);}
    
  
 
  /** @generated */
  final Feature casFeat_docseq;
  /** @generated */
  final int     casFeatCode_docseq;
  /** @generated */ 
  public int getDocseq(int addr) {
        if (featOkTst && casFeat_docseq == null)
      jcas.throwFeatMissing("docseq", "org.apache.uima.ducc.sampleapps.DuccDocumentInfo");
    return ll_cas.ll_getIntValue(addr, casFeatCode_docseq);
  }
  /** @generated */    
  public void setDocseq(int addr, int v) {
        if (featOkTst && casFeat_docseq == null)
      jcas.throwFeatMissing("docseq", "org.apache.uima.ducc.sampleapps.DuccDocumentInfo");
    ll_cas.ll_setIntValue(addr, casFeatCode_docseq, v);}
    
  
 
  /** @generated */
  final Feature casFeat_byteoffset;
  /** @generated */
  final int     casFeatCode_byteoffset;
  /** @generated */ 
  public int getByteoffset(int addr) {
        if (featOkTst && casFeat_byteoffset == null)
      jcas.throwFeatMissing("byteoffset", "org.apache.uima.ducc.sampleapps.DuccDocumentInfo");
    return ll_cas.ll_getIntValue(addr, casFeatCode_byteoffset);
  }
  /** @generated */    
  public void setByteoffset(int addr, int v) {
        if (featOkTst && casFeat_byteoffset == null)
      jcas.throwFeatMissing("byteoffset", "org.apache.uima.ducc.sampleapps.DuccDocumentInfo");
    ll_cas.ll_setIntValue(addr, casFeatCode_byteoffset, v);}
    
  



  /** initialize variables to correspond with Cas Type and Features
	* @generated */
  public DuccDocumentInfo_Type(JCas jcas, Type casType) {
    super(jcas, casType);
    casImpl.getFSClassRegistry().addGeneratorForType((TypeImpl)this.casType, getFSGenerator());

 
    casFeat_inputfile = jcas.getRequiredFeatureDE(casType, "inputfile", "uima.cas.String", featOkTst);
    casFeatCode_inputfile  = (null == casFeat_inputfile) ? JCas.INVALID_FEATURE_CODE : ((FeatureImpl)casFeat_inputfile).getCode();

 
    casFeat_outputfile = jcas.getRequiredFeatureDE(casType, "outputfile", "uima.cas.String", featOkTst);
    casFeatCode_outputfile  = (null == casFeat_outputfile) ? JCas.INVALID_FEATURE_CODE : ((FeatureImpl)casFeat_outputfile).getCode();

 
    casFeat_docseq = jcas.getRequiredFeatureDE(casType, "docseq", "uima.cas.Integer", featOkTst);
    casFeatCode_docseq  = (null == casFeat_docseq) ? JCas.INVALID_FEATURE_CODE : ((FeatureImpl)casFeat_docseq).getCode();

 
    casFeat_byteoffset = jcas.getRequiredFeatureDE(casType, "byteoffset", "uima.cas.Integer", featOkTst);
    casFeatCode_byteoffset  = (null == casFeat_byteoffset) ? JCas.INVALID_FEATURE_CODE : ((FeatureImpl)casFeat_byteoffset).getCode();

  }
}



    