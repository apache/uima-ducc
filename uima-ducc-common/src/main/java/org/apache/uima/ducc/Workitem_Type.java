
/* First created by JCasGen Fri Jul 19 18:26:47 EDT 2013 */
package org.apache.uima.ducc;

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
 * Updated by JCasGen Fri Jul 19 18:26:47 EDT 2013
 * @generated */
public class Workitem_Type extends TOP_Type {
  /** @generated */
  protected FSGenerator getFSGenerator() {return fsGenerator;}
  /** @generated */
  private final FSGenerator fsGenerator = 
    new FSGenerator() {
      public FeatureStructure createFS(int addr, CASImpl cas) {
  			 if (Workitem_Type.this.useExistingInstance) {
  			   // Return eq fs instance if already created
  		     FeatureStructure fs = Workitem_Type.this.jcas.getJfsFromCaddr(addr);
  		     if (null == fs) {
  		       fs = new Workitem(addr, Workitem_Type.this);
  			   Workitem_Type.this.jcas.putJfsFromCaddr(addr, fs);
  			   return fs;
  		     }
  		     return fs;
        } else return new Workitem(addr, Workitem_Type.this);
  	  }
    };
  /** @generated */
  public final static int typeIndexID = Workitem.typeIndexID;
  /** @generated 
     @modifiable */
  public final static boolean featOkTst = JCasRegistry.getFeatOkTst("org.apache.uima.ducc.Workitem");
 
  /** @generated */
  final Feature casFeat_inputfile;
  /** @generated */
  final int     casFeatCode_inputfile;
  /** @generated */ 
  public String getInputfile(int addr) {
        if (featOkTst && casFeat_inputfile == null)
      jcas.throwFeatMissing("inputfile", "org.apache.uima.ducc.Workitem");
    return ll_cas.ll_getStringValue(addr, casFeatCode_inputfile);
  }
  /** @generated */    
  public void setInputfile(int addr, String v) {
        if (featOkTst && casFeat_inputfile == null)
      jcas.throwFeatMissing("inputfile", "org.apache.uima.ducc.Workitem");
    ll_cas.ll_setStringValue(addr, casFeatCode_inputfile, v);}
    
  
 
  /** @generated */
  final Feature casFeat_outputfile;
  /** @generated */
  final int     casFeatCode_outputfile;
  /** @generated */ 
  public String getOutputfile(int addr) {
        if (featOkTst && casFeat_outputfile == null)
      jcas.throwFeatMissing("outputfile", "org.apache.uima.ducc.Workitem");
    return ll_cas.ll_getStringValue(addr, casFeatCode_outputfile);
  }
  /** @generated */    
  public void setOutputfile(int addr, String v) {
        if (featOkTst && casFeat_outputfile == null)
      jcas.throwFeatMissing("outputfile", "org.apache.uima.ducc.Workitem");
    ll_cas.ll_setStringValue(addr, casFeatCode_outputfile, v);}
    
  
 
  /** @generated */
  final Feature casFeat_encoding;
  /** @generated */
  final int     casFeatCode_encoding;
  /** @generated */ 
  public String getEncoding(int addr) {
        if (featOkTst && casFeat_encoding == null)
      jcas.throwFeatMissing("encoding", "org.apache.uima.ducc.Workitem");
    return ll_cas.ll_getStringValue(addr, casFeatCode_encoding);
  }
  /** @generated */    
  public void setEncoding(int addr, String v) {
        if (featOkTst && casFeat_encoding == null)
      jcas.throwFeatMissing("encoding", "org.apache.uima.ducc.Workitem");
    ll_cas.ll_setStringValue(addr, casFeatCode_encoding, v);}
    
  
 
  /** @generated */
  final Feature casFeat_language;
  /** @generated */
  final int     casFeatCode_language;
  /** @generated */ 
  public String getLanguage(int addr) {
        if (featOkTst && casFeat_language == null)
      jcas.throwFeatMissing("language", "org.apache.uima.ducc.Workitem");
    return ll_cas.ll_getStringValue(addr, casFeatCode_language);
  }
  /** @generated */    
  public void setLanguage(int addr, String v) {
        if (featOkTst && casFeat_language == null)
      jcas.throwFeatMissing("language", "org.apache.uima.ducc.Workitem");
    ll_cas.ll_setStringValue(addr, casFeatCode_language, v);}
    
  
 
  /** @generated */
  final Feature casFeat_blocksize;
  /** @generated */
  final int     casFeatCode_blocksize;
  /** @generated */ 
  public int getBlocksize(int addr) {
        if (featOkTst && casFeat_blocksize == null)
      jcas.throwFeatMissing("blocksize", "org.apache.uima.ducc.Workitem");
    return ll_cas.ll_getIntValue(addr, casFeatCode_blocksize);
  }
  /** @generated */    
  public void setBlocksize(int addr, int v) {
        if (featOkTst && casFeat_blocksize == null)
      jcas.throwFeatMissing("blocksize", "org.apache.uima.ducc.Workitem");
    ll_cas.ll_setIntValue(addr, casFeatCode_blocksize, v);}
    
  
 
  /** @generated */
  final Feature casFeat_blockindex;
  /** @generated */
  final int     casFeatCode_blockindex;
  /** @generated */ 
  public int getBlockindex(int addr) {
        if (featOkTst && casFeat_blockindex == null)
      jcas.throwFeatMissing("blockindex", "org.apache.uima.ducc.Workitem");
    return ll_cas.ll_getIntValue(addr, casFeatCode_blockindex);
  }
  /** @generated */    
  public void setBlockindex(int addr, int v) {
        if (featOkTst && casFeat_blockindex == null)
      jcas.throwFeatMissing("blockindex", "org.apache.uima.ducc.Workitem");
    ll_cas.ll_setIntValue(addr, casFeatCode_blockindex, v);}
    
  
 
  /** @generated */
  final Feature casFeat_sendToCC;
  /** @generated */
  final int     casFeatCode_sendToCC;
  /** @generated */ 
  public boolean getSendToCC(int addr) {
        if (featOkTst && casFeat_sendToCC == null)
      jcas.throwFeatMissing("sendToCC", "org.apache.uima.ducc.Workitem");
    return ll_cas.ll_getBooleanValue(addr, casFeatCode_sendToCC);
  }
  /** @generated */    
  public void setSendToCC(int addr, boolean v) {
        if (featOkTst && casFeat_sendToCC == null)
      jcas.throwFeatMissing("sendToCC", "org.apache.uima.ducc.Workitem");
    ll_cas.ll_setBooleanValue(addr, casFeatCode_sendToCC, v);}
    
  



  /** initialize variables to correspond with Cas Type and Features
	* @generated */
  public Workitem_Type(JCas jcas, Type casType) {
    super(jcas, casType);
    casImpl.getFSClassRegistry().addGeneratorForType((TypeImpl)this.casType, getFSGenerator());

 
    casFeat_inputfile = jcas.getRequiredFeatureDE(casType, "inputfile", "uima.cas.String", featOkTst);
    casFeatCode_inputfile  = (null == casFeat_inputfile) ? JCas.INVALID_FEATURE_CODE : ((FeatureImpl)casFeat_inputfile).getCode();

 
    casFeat_outputfile = jcas.getRequiredFeatureDE(casType, "outputfile", "uima.cas.String", featOkTst);
    casFeatCode_outputfile  = (null == casFeat_outputfile) ? JCas.INVALID_FEATURE_CODE : ((FeatureImpl)casFeat_outputfile).getCode();

 
    casFeat_encoding = jcas.getRequiredFeatureDE(casType, "encoding", "uima.cas.String", featOkTst);
    casFeatCode_encoding  = (null == casFeat_encoding) ? JCas.INVALID_FEATURE_CODE : ((FeatureImpl)casFeat_encoding).getCode();

 
    casFeat_language = jcas.getRequiredFeatureDE(casType, "language", "uima.cas.String", featOkTst);
    casFeatCode_language  = (null == casFeat_language) ? JCas.INVALID_FEATURE_CODE : ((FeatureImpl)casFeat_language).getCode();

 
    casFeat_blocksize = jcas.getRequiredFeatureDE(casType, "blocksize", "uima.cas.Integer", featOkTst);
    casFeatCode_blocksize  = (null == casFeat_blocksize) ? JCas.INVALID_FEATURE_CODE : ((FeatureImpl)casFeat_blocksize).getCode();

 
    casFeat_blockindex = jcas.getRequiredFeatureDE(casType, "blockindex", "uima.cas.Integer", featOkTst);
    casFeatCode_blockindex  = (null == casFeat_blockindex) ? JCas.INVALID_FEATURE_CODE : ((FeatureImpl)casFeat_blockindex).getCode();

 
    casFeat_sendToCC = jcas.getRequiredFeatureDE(casType, "sendToCC", "uima.cas.Boolean", featOkTst);
    casFeatCode_sendToCC  = (null == casFeat_sendToCC) ? JCas.INVALID_FEATURE_CODE : ((FeatureImpl)casFeat_sendToCC).getCode();

  }
}



    