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

   
/* Apache UIMA v3 - First created by JCasGen Thu Jan 31 15:34:33 EST 2019 */

package org.apache.uima.ducc;

import java.lang.invoke.CallSite;
import java.lang.invoke.MethodHandle;

import org.apache.uima.cas.impl.CASImpl;
import org.apache.uima.cas.impl.TypeImpl;
import org.apache.uima.cas.impl.TypeSystemImpl;
import org.apache.uima.jcas.JCas; 
import org.apache.uima.jcas.JCasRegistry;


import org.apache.uima.jcas.cas.TOP;


/** 
 * Updated by JCasGen Thu Jan 31 15:34:33 EST 2019
 * XML source: /home/eddie/workspaces/workspace-ducc/uima-ducc/uima-ducc-user/src/main/resources/org/apache/uima/ducc/FlowControllerTS.xml
 * @generated */
public class Workitem extends TOP {
 
  /** @generated
   * @ordered 
   */
  @SuppressWarnings ("hiding")
  public final static String _TypeName = "org.apache.uima.ducc.Workitem";
  
  /** @generated
   * @ordered 
   */
  @SuppressWarnings ("hiding")
  public final static int typeIndexID = JCasRegistry.register(Workitem.class);
  /** @generated
   * @ordered 
   */
  @SuppressWarnings ("hiding")
  public final static int type = typeIndexID;
  /** @generated
   * @return index of the type  
   */
  @Override
  public              int getTypeIndexID() {return typeIndexID;}
 
 
  /* *******************
   *   Feature Offsets *
   * *******************/ 
   
  public final static String _FeatName_sendToLast = "sendToLast";
  public final static String _FeatName_sendToAll = "sendToAll";
  public final static String _FeatName_inputspec = "inputspec";
  public final static String _FeatName_outputspec = "outputspec";
  public final static String _FeatName_encoding = "encoding";
  public final static String _FeatName_language = "language";
  public final static String _FeatName_bytelength = "bytelength";
  public final static String _FeatName_blockindex = "blockindex";
  public final static String _FeatName_blocksize = "blocksize";
  public final static String _FeatName_lastBlock = "lastBlock";


  /* Feature Adjusted Offsets */
  private final static CallSite _FC_sendToLast = TypeSystemImpl.createCallSite(Workitem.class, "sendToLast");
  private final static MethodHandle _FH_sendToLast = _FC_sendToLast.dynamicInvoker();
  private final static CallSite _FC_sendToAll = TypeSystemImpl.createCallSite(Workitem.class, "sendToAll");
  private final static MethodHandle _FH_sendToAll = _FC_sendToAll.dynamicInvoker();
  private final static CallSite _FC_inputspec = TypeSystemImpl.createCallSite(Workitem.class, "inputspec");
  private final static MethodHandle _FH_inputspec = _FC_inputspec.dynamicInvoker();
  private final static CallSite _FC_outputspec = TypeSystemImpl.createCallSite(Workitem.class, "outputspec");
  private final static MethodHandle _FH_outputspec = _FC_outputspec.dynamicInvoker();
  private final static CallSite _FC_encoding = TypeSystemImpl.createCallSite(Workitem.class, "encoding");
  private final static MethodHandle _FH_encoding = _FC_encoding.dynamicInvoker();
  private final static CallSite _FC_language = TypeSystemImpl.createCallSite(Workitem.class, "language");
  private final static MethodHandle _FH_language = _FC_language.dynamicInvoker();
  private final static CallSite _FC_bytelength = TypeSystemImpl.createCallSite(Workitem.class, "bytelength");
  private final static MethodHandle _FH_bytelength = _FC_bytelength.dynamicInvoker();
  private final static CallSite _FC_blockindex = TypeSystemImpl.createCallSite(Workitem.class, "blockindex");
  private final static MethodHandle _FH_blockindex = _FC_blockindex.dynamicInvoker();
  private final static CallSite _FC_blocksize = TypeSystemImpl.createCallSite(Workitem.class, "blocksize");
  private final static MethodHandle _FH_blocksize = _FC_blocksize.dynamicInvoker();
  private final static CallSite _FC_lastBlock = TypeSystemImpl.createCallSite(Workitem.class, "lastBlock");
  private final static MethodHandle _FH_lastBlock = _FC_lastBlock.dynamicInvoker();

   
  /** Never called.  Disable default constructor
   * @generated */
  protected Workitem() {/* intentionally empty block */}
    
  /** Internal - constructor used by generator 
   * @generated
   * @param casImpl the CAS this Feature Structure belongs to
   * @param type the type of this Feature Structure 
   */
  public Workitem(TypeImpl type, CASImpl casImpl) {
    super(type, casImpl);
    readObject();
  }
  
  /** @generated
   * @param jcas JCas to which this Feature Structure belongs 
   */
  public Workitem(JCas jcas) {
    super(jcas);
    readObject();   
  } 


  /** 
   * <!-- begin-user-doc -->
   * Write your own initialization here
   * <!-- end-user-doc -->
   *
   * @generated modifiable 
   */
  private void readObject() {/*default - does nothing empty block */}
     
 
    
  //*--------------*
  //* Feature: sendToLast

  /** getter for sendToLast - gets 
   * @generated
   * @return value of the feature 
   */
  public boolean getSendToLast() { return _getBooleanValueNc(wrapGetIntCatchException(_FH_sendToLast));}
    
  /** setter for sendToLast - sets  
   * @generated
   * @param v value to set into the feature 
   */
  public void setSendToLast(boolean v) {
    _setBooleanValueNfc(wrapGetIntCatchException(_FH_sendToLast), v);
  }    
    
   
    
  //*--------------*
  //* Feature: sendToAll

  /** getter for sendToAll - gets 
   * @generated
   * @return value of the feature 
   */
  public boolean getSendToAll() { return _getBooleanValueNc(wrapGetIntCatchException(_FH_sendToAll));}
    
  /** setter for sendToAll - sets  
   * @generated
   * @param v value to set into the feature 
   */
  public void setSendToAll(boolean v) {
    _setBooleanValueNfc(wrapGetIntCatchException(_FH_sendToAll), v);
  }    
    
   
    
  //*--------------*
  //* Feature: inputspec

  /** getter for inputspec - gets 
   * @generated
   * @return value of the feature 
   */
  public String getInputspec() { return _getStringValueNc(wrapGetIntCatchException(_FH_inputspec));}
    
  /** setter for inputspec - sets  
   * @generated
   * @param v value to set into the feature 
   */
  public void setInputspec(String v) {
    _setStringValueNfc(wrapGetIntCatchException(_FH_inputspec), v);
  }    
    
   
    
  //*--------------*
  //* Feature: outputspec

  /** getter for outputspec - gets 
   * @generated
   * @return value of the feature 
   */
  public String getOutputspec() { return _getStringValueNc(wrapGetIntCatchException(_FH_outputspec));}
    
  /** setter for outputspec - sets  
   * @generated
   * @param v value to set into the feature 
   */
  public void setOutputspec(String v) {
    _setStringValueNfc(wrapGetIntCatchException(_FH_outputspec), v);
  }    
    
   
    
  //*--------------*
  //* Feature: encoding

  /** getter for encoding - gets Optional parameter to use when converting input files into Java characters
   * @generated
   * @return value of the feature 
   */
  public String getEncoding() { return _getStringValueNc(wrapGetIntCatchException(_FH_encoding));}
    
  /** setter for encoding - sets Optional parameter to use when converting input files into Java characters 
   * @generated
   * @param v value to set into the feature 
   */
  public void setEncoding(String v) {
    _setStringValueNfc(wrapGetIntCatchException(_FH_encoding), v);
  }    
    
   
    
  //*--------------*
  //* Feature: language

  /** getter for language - gets Optional parameter to specify the text language
   * @generated
   * @return value of the feature 
   */
  public String getLanguage() { return _getStringValueNc(wrapGetIntCatchException(_FH_language));}
    
  /** setter for language - sets Optional parameter to specify the text language 
   * @generated
   * @param v value to set into the feature 
   */
  public void setLanguage(String v) {
    _setStringValueNfc(wrapGetIntCatchException(_FH_language), v);
  }    
    
   
    
  //*--------------*
  //* Feature: bytelength

  /** getter for bytelength - gets Length in bytes of work item
   * @generated
   * @return value of the feature 
   */
  public int getBytelength() { return _getIntValueNc(wrapGetIntCatchException(_FH_bytelength));}
    
  /** setter for bytelength - sets Length in bytes of work item 
   * @generated
   * @param v value to set into the feature 
   */
  public void setBytelength(int v) {
    _setIntValueNfc(wrapGetIntCatchException(_FH_bytelength), v);
  }    
    
   
    
  //*--------------*
  //* Feature: blockindex

  /** getter for blockindex - gets Optional parameter to specify block sequence number for input file
   * @generated
   * @return value of the feature 
   */
  public int getBlockindex() { return _getIntValueNc(wrapGetIntCatchException(_FH_blockindex));}
    
  /** setter for blockindex - sets Optional parameter to specify block sequence number for input file 
   * @generated
   * @param v value to set into the feature 
   */
  public void setBlockindex(int v) {
    _setIntValueNfc(wrapGetIntCatchException(_FH_blockindex), v);
  }    
    
   
    
  //*--------------*
  //* Feature: blocksize

  /** getter for blocksize - gets Optional parameter to specify block size
   * @generated
   * @return value of the feature 
   */
  public int getBlocksize() { return _getIntValueNc(wrapGetIntCatchException(_FH_blocksize));}
    
  /** setter for blocksize - sets Optional parameter to specify block size 
   * @generated
   * @param v value to set into the feature 
   */
  public void setBlocksize(int v) {
    _setIntValueNfc(wrapGetIntCatchException(_FH_blocksize), v);
  }    
    
   
    
  //*--------------*
  //* Feature: lastBlock

  /** getter for lastBlock - gets Optional parameter to specify this is last chunk for work item
   * @generated
   * @return value of the feature 
   */
  public boolean getLastBlock() { return _getBooleanValueNc(wrapGetIntCatchException(_FH_lastBlock));}
    
  /** setter for lastBlock - sets Optional parameter to specify this is last chunk for work item 
   * @generated
   * @param v value to set into the feature 
   */
  public void setLastBlock(boolean v) {
    _setBooleanValueNfc(wrapGetIntCatchException(_FH_lastBlock), v);
  }    
    
  }

    
