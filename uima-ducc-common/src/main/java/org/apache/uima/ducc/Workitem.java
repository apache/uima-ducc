

/* First created by JCasGen Fri Jul 19 18:26:47 EDT 2013 */
package org.apache.uima.ducc;

import org.apache.uima.jcas.JCas; 
import org.apache.uima.jcas.JCasRegistry;
import org.apache.uima.jcas.cas.TOP_Type;

import org.apache.uima.jcas.cas.TOP;


/** 
 * Updated by JCasGen Fri Jul 19 18:26:47 EDT 2013
 * XML source: /users1/eae/workspace-ducc/uima-ducc/uima-ducc-common/src/main/java/org/apache/uima/ducc/common/uima/DuccJobFlowControlTS.xml
 * @generated */
public class Workitem extends TOP {
  /** @generated
   * @ordered 
   */
  public final static int typeIndexID = JCasRegistry.register(Workitem.class);
  /** @generated
   * @ordered 
   */
  public final static int type = typeIndexID;
  /** @generated  */
  public              int getTypeIndexID() {return typeIndexID;}
 
  /** Never called.  Disable default constructor
   * @generated */
  protected Workitem() {}
    
  /** Internal - constructor used by generator 
   * @generated */
  public Workitem(int addr, TOP_Type type) {
    super(addr, type);
    readObject();
  }
  
  /** @generated */
  public Workitem(JCas jcas) {
    super(jcas);
    readObject();   
  } 

  /** <!-- begin-user-doc -->
    * Write your own initialization here
    * <!-- end-user-doc -->
  @generated modifiable */
  private void readObject() {}
     
 
    
  //*--------------*
  //* Feature: inputfile

  /** getter for inputfile - gets 
   * @generated */
  public String getInputfile() {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_inputfile == null)
      jcasType.jcas.throwFeatMissing("inputfile", "org.apache.uima.ducc.Workitem");
    return jcasType.ll_cas.ll_getStringValue(addr, ((Workitem_Type)jcasType).casFeatCode_inputfile);}
    
  /** setter for inputfile - sets  
   * @generated */
  public void setInputfile(String v) {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_inputfile == null)
      jcasType.jcas.throwFeatMissing("inputfile", "org.apache.uima.ducc.Workitem");
    jcasType.ll_cas.ll_setStringValue(addr, ((Workitem_Type)jcasType).casFeatCode_inputfile, v);}    
   
    
  //*--------------*
  //* Feature: outputfile

  /** getter for outputfile - gets 
   * @generated */
  public String getOutputfile() {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_outputfile == null)
      jcasType.jcas.throwFeatMissing("outputfile", "org.apache.uima.ducc.Workitem");
    return jcasType.ll_cas.ll_getStringValue(addr, ((Workitem_Type)jcasType).casFeatCode_outputfile);}
    
  /** setter for outputfile - sets  
   * @generated */
  public void setOutputfile(String v) {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_outputfile == null)
      jcasType.jcas.throwFeatMissing("outputfile", "org.apache.uima.ducc.Workitem");
    jcasType.ll_cas.ll_setStringValue(addr, ((Workitem_Type)jcasType).casFeatCode_outputfile, v);}    
   
    
  //*--------------*
  //* Feature: encoding

  /** getter for encoding - gets Optional parameter to use when converting input files into Java characters
   * @generated */
  public String getEncoding() {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_encoding == null)
      jcasType.jcas.throwFeatMissing("encoding", "org.apache.uima.ducc.Workitem");
    return jcasType.ll_cas.ll_getStringValue(addr, ((Workitem_Type)jcasType).casFeatCode_encoding);}
    
  /** setter for encoding - sets Optional parameter to use when converting input files into Java characters 
   * @generated */
  public void setEncoding(String v) {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_encoding == null)
      jcasType.jcas.throwFeatMissing("encoding", "org.apache.uima.ducc.Workitem");
    jcasType.ll_cas.ll_setStringValue(addr, ((Workitem_Type)jcasType).casFeatCode_encoding, v);}    
   
    
  //*--------------*
  //* Feature: language

  /** getter for language - gets Optional parameter to specify the text language
   * @generated */
  public String getLanguage() {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_language == null)
      jcasType.jcas.throwFeatMissing("language", "org.apache.uima.ducc.Workitem");
    return jcasType.ll_cas.ll_getStringValue(addr, ((Workitem_Type)jcasType).casFeatCode_language);}
    
  /** setter for language - sets Optional parameter to specify the text language 
   * @generated */
  public void setLanguage(String v) {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_language == null)
      jcasType.jcas.throwFeatMissing("language", "org.apache.uima.ducc.Workitem");
    jcasType.ll_cas.ll_setStringValue(addr, ((Workitem_Type)jcasType).casFeatCode_language, v);}    
   
    
  //*--------------*
  //* Feature: blocksize

  /** getter for blocksize - gets Optional parameter to process input files in smaller chunks
   * @generated */
  public int getBlocksize() {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_blocksize == null)
      jcasType.jcas.throwFeatMissing("blocksize", "org.apache.uima.ducc.Workitem");
    return jcasType.ll_cas.ll_getIntValue(addr, ((Workitem_Type)jcasType).casFeatCode_blocksize);}
    
  /** setter for blocksize - sets Optional parameter to process input files in smaller chunks 
   * @generated */
  public void setBlocksize(int v) {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_blocksize == null)
      jcasType.jcas.throwFeatMissing("blocksize", "org.apache.uima.ducc.Workitem");
    jcasType.ll_cas.ll_setIntValue(addr, ((Workitem_Type)jcasType).casFeatCode_blocksize, v);}    
   
    
  //*--------------*
  //* Feature: blockindex

  /** getter for blockindex - gets Optional parameter to specify block offset into input file to start processing
   * @generated */
  public int getBlockindex() {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_blockindex == null)
      jcasType.jcas.throwFeatMissing("blockindex", "org.apache.uima.ducc.Workitem");
    return jcasType.ll_cas.ll_getIntValue(addr, ((Workitem_Type)jcasType).casFeatCode_blockindex);}
    
  /** setter for blockindex - sets Optional parameter to specify block offset into input file to start processing 
   * @generated */
  public void setBlockindex(int v) {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_blockindex == null)
      jcasType.jcas.throwFeatMissing("blockindex", "org.apache.uima.ducc.Workitem");
    jcasType.ll_cas.ll_setIntValue(addr, ((Workitem_Type)jcasType).casFeatCode_blockindex, v);}    
   
    
  //*--------------*
  //* Feature: sendToCC

  /** getter for sendToCC - gets 
   * @generated */
  public boolean getSendToCC() {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_sendToCC == null)
      jcasType.jcas.throwFeatMissing("sendToCC", "org.apache.uima.ducc.Workitem");
    return jcasType.ll_cas.ll_getBooleanValue(addr, ((Workitem_Type)jcasType).casFeatCode_sendToCC);}
    
  /** setter for sendToCC - sets  
   * @generated */
  public void setSendToCC(boolean v) {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_sendToCC == null)
      jcasType.jcas.throwFeatMissing("sendToCC", "org.apache.uima.ducc.Workitem");
    jcasType.ll_cas.ll_setBooleanValue(addr, ((Workitem_Type)jcasType).casFeatCode_sendToCC, v);}    
  }

    