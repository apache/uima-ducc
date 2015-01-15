/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
*/

/* First created by JCasGen Fri Jul 19 18:26:47 EDT 2013 */
package org.apache.uima.ducc;

import org.apache.uima.jcas.JCas; 
import org.apache.uima.jcas.JCasRegistry;
import org.apache.uima.jcas.cas.TOP_Type;

import org.apache.uima.jcas.cas.TOP;


@Deprecated // ducc 2.0
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
  //* Feature: sendToLast

  /** getter for sendToLast - gets 
   * @generated */
  public boolean getSendToLast() {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_sendToLast == null)
      jcasType.jcas.throwFeatMissing("sendToLast", "org.apache.uima.ducc.Workitem");
    return jcasType.ll_cas.ll_getBooleanValue(addr, ((Workitem_Type)jcasType).casFeatCode_sendToLast);}
    
  /** setter for sendToLast - sets  
   * @generated */
  public void setSendToLast(boolean v) {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_sendToLast == null)
      jcasType.jcas.throwFeatMissing("sendToLast", "org.apache.uima.ducc.Workitem");
    jcasType.ll_cas.ll_setBooleanValue(addr, ((Workitem_Type)jcasType).casFeatCode_sendToLast, v);}    
   
    
  //*--------------*
  //* Feature: sendToAll

  /** getter for sendToAll - gets 
   * @generated */
  public boolean getSendToAll() {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_sendToAll == null)
      jcasType.jcas.throwFeatMissing("sendToAll", "org.apache.uima.ducc.Workitem");
    return jcasType.ll_cas.ll_getBooleanValue(addr, ((Workitem_Type)jcasType).casFeatCode_sendToAll);}
    
  /** setter for sendToAll - sets  
   * @generated */
  public void setSendToAll(boolean v) {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_sendToAll == null)
      jcasType.jcas.throwFeatMissing("sendToAll", "org.apache.uima.ducc.Workitem");
    jcasType.ll_cas.ll_setBooleanValue(addr, ((Workitem_Type)jcasType).casFeatCode_sendToAll, v);}    
   
    
  //*--------------*
  //* Feature: inputspec

  /** getter for inputspec - gets 
   * @generated */
  public String getInputspec() {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_inputspec == null)
      jcasType.jcas.throwFeatMissing("inputspec", "org.apache.uima.ducc.Workitem");
    return jcasType.ll_cas.ll_getStringValue(addr, ((Workitem_Type)jcasType).casFeatCode_inputspec);}
    
  /** setter for inputspec - sets  
   * @generated */
  public void setInputspec(String v) {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_inputspec == null)
      jcasType.jcas.throwFeatMissing("inputspec", "org.apache.uima.ducc.Workitem");
    jcasType.ll_cas.ll_setStringValue(addr, ((Workitem_Type)jcasType).casFeatCode_inputspec, v);}    
   
    
  //*--------------*
  //* Feature: outputspec

  /** getter for outputspec - gets 
   * @generated */
  public String getOutputspec() {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_outputspec == null)
      jcasType.jcas.throwFeatMissing("outputspec", "org.apache.uima.ducc.Workitem");
    return jcasType.ll_cas.ll_getStringValue(addr, ((Workitem_Type)jcasType).casFeatCode_outputspec);}
    
  /** setter for outputspec - sets  
   * @generated */
  public void setOutputspec(String v) {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_outputspec == null)
      jcasType.jcas.throwFeatMissing("outputspec", "org.apache.uima.ducc.Workitem");
    jcasType.ll_cas.ll_setStringValue(addr, ((Workitem_Type)jcasType).casFeatCode_outputspec, v);}    
   
    
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
  //* Feature: bytelength

  /** getter for bytelength - gets Length in bytes of work item
   * @generated */
  public int getBytelength() {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_bytelength == null)
      jcasType.jcas.throwFeatMissing("bytelength", "org.apache.uima.ducc.Workitem");
    return jcasType.ll_cas.ll_getIntValue(addr, ((Workitem_Type)jcasType).casFeatCode_bytelength);}
    
  /** setter for bytelength - sets Length in bytes of work item 
   * @generated */
  public void setBytelength(int v) {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_bytelength == null)
      jcasType.jcas.throwFeatMissing("bytelength", "org.apache.uima.ducc.Workitem");
    jcasType.ll_cas.ll_setIntValue(addr, ((Workitem_Type)jcasType).casFeatCode_bytelength, v);}    
   
    
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
  //* Feature: lastBlock

  /** getter for lastBlock - gets 
   * @generated */
  public boolean getLastBlock() {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_lastBlock == null)
      jcasType.jcas.throwFeatMissing("lastBlock", "org.apache.uima.ducc.Workitem");
    return jcasType.ll_cas.ll_getBooleanValue(addr, ((Workitem_Type)jcasType).casFeatCode_lastBlock);}
    
  /** setter for lastBlock - sets  
   * @generated */
  public void setLastBlock(boolean v) {
    if (Workitem_Type.featOkTst && ((Workitem_Type)jcasType).casFeat_lastBlock == null)
      jcasType.jcas.throwFeatMissing("lastBlock", "org.apache.uima.ducc.Workitem");
    jcasType.ll_cas.ll_setBooleanValue(addr, ((Workitem_Type)jcasType).casFeatCode_lastBlock, v);}    
  }

    