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
import org.apache.uima.cas.impl.CASImpl;
import org.apache.uima.cas.impl.FSGenerator;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.cas.impl.TypeImpl;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.impl.FeatureImpl;
import org.apache.uima.cas.Feature;
import org.apache.uima.jcas.cas.TOP_Type;

/**
 * Updated by JCasGen Fri Aug 02 09:06:21 EDT 2013
 * 
 * @generated
 */
public class Workitem_Type extends TOP_Type {
  /** @generated */
  protected FSGenerator getFSGenerator() {
    return fsGenerator;
  }

  /** @generated */
  private final FSGenerator fsGenerator = new FSGenerator() {
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
      } else
        return new Workitem(addr, Workitem_Type.this);
    }
  };

  /** @generated */
  public final static int typeIndexID = Workitem.typeIndexID;

  /**
   * @generated
   * @modifiable
   */
  public final static boolean featOkTst = JCasRegistry
          .getFeatOkTst("org.apache.uima.ducc.Workitem");

  /** @generated */
  final Feature casFeat_sendToLast;

  /** @generated */
  final int casFeatCode_sendToLast;

  /** @generated */
  public boolean getSendToLast(int addr) {
    if (featOkTst && casFeat_sendToLast == null)
      jcas.throwFeatMissing("sendToLast", "org.apache.uima.ducc.Workitem");
    return ll_cas.ll_getBooleanValue(addr, casFeatCode_sendToLast);
  }

  /** @generated */
  public void setSendToLast(int addr, boolean v) {
    if (featOkTst && casFeat_sendToLast == null)
      jcas.throwFeatMissing("sendToLast", "org.apache.uima.ducc.Workitem");
    ll_cas.ll_setBooleanValue(addr, casFeatCode_sendToLast, v);
  }

  /** @generated */
  final Feature casFeat_sendToAll;

  /** @generated */
  final int casFeatCode_sendToAll;

  /** @generated */
  public boolean getSendToAll(int addr) {
    if (featOkTst && casFeat_sendToAll == null)
      jcas.throwFeatMissing("sendToAll", "org.apache.uima.ducc.Workitem");
    return ll_cas.ll_getBooleanValue(addr, casFeatCode_sendToAll);
  }

  /** @generated */
  public void setSendToAll(int addr, boolean v) {
    if (featOkTst && casFeat_sendToAll == null)
      jcas.throwFeatMissing("sendToAll", "org.apache.uima.ducc.Workitem");
    ll_cas.ll_setBooleanValue(addr, casFeatCode_sendToAll, v);
  }

  /** @generated */
  final Feature casFeat_inputspec;

  /** @generated */
  final int casFeatCode_inputspec;

  /** @generated */
  public String getInputspec(int addr) {
    if (featOkTst && casFeat_inputspec == null)
      jcas.throwFeatMissing("inputspec", "org.apache.uima.ducc.Workitem");
    return ll_cas.ll_getStringValue(addr, casFeatCode_inputspec);
  }

  /** @generated */
  public void setInputspec(int addr, String v) {
    if (featOkTst && casFeat_inputspec == null)
      jcas.throwFeatMissing("inputspec", "org.apache.uima.ducc.Workitem");
    ll_cas.ll_setStringValue(addr, casFeatCode_inputspec, v);
  }

  /** @generated */
  final Feature casFeat_outputspec;

  /** @generated */
  final int casFeatCode_outputspec;

  /** @generated */
  public String getOutputspec(int addr) {
    if (featOkTst && casFeat_outputspec == null)
      jcas.throwFeatMissing("outputspec", "org.apache.uima.ducc.Workitem");
    return ll_cas.ll_getStringValue(addr, casFeatCode_outputspec);
  }

  /** @generated */
  public void setOutputspec(int addr, String v) {
    if (featOkTst && casFeat_outputspec == null)
      jcas.throwFeatMissing("outputspec", "org.apache.uima.ducc.Workitem");
    ll_cas.ll_setStringValue(addr, casFeatCode_outputspec, v);
  }

  /** @generated */
  final Feature casFeat_encoding;

  /** @generated */
  final int casFeatCode_encoding;

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
    ll_cas.ll_setStringValue(addr, casFeatCode_encoding, v);
  }

  /** @generated */
  final Feature casFeat_language;

  /** @generated */
  final int casFeatCode_language;

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
    ll_cas.ll_setStringValue(addr, casFeatCode_language, v);
  }

  /** @generated */
  final Feature casFeat_bytelength;

  /** @generated */
  final int casFeatCode_bytelength;

  /** @generated */
  public int getBytelength(int addr) {
    if (featOkTst && casFeat_bytelength == null)
      jcas.throwFeatMissing("bytelength", "org.apache.uima.ducc.Workitem");
    return ll_cas.ll_getIntValue(addr, casFeatCode_bytelength);
  }

  /** @generated */
  public void setBytelength(int addr, int v) {
    if (featOkTst && casFeat_bytelength == null)
      jcas.throwFeatMissing("bytelength", "org.apache.uima.ducc.Workitem");
    ll_cas.ll_setIntValue(addr, casFeatCode_bytelength, v);
  }

  /** @generated */
  final Feature casFeat_blocksize;

  /** @generated */
  final int casFeatCode_blocksize;

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
    ll_cas.ll_setIntValue(addr, casFeatCode_blocksize, v);
  }

  /** @generated */
  final Feature casFeat_blockindex;

  /** @generated */
  final int casFeatCode_blockindex;

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
    ll_cas.ll_setIntValue(addr, casFeatCode_blockindex, v);
  }

  /** @generated */
  final Feature casFeat_lastBlock;

  /** @generated */
  final int casFeatCode_lastBlock;

  /** @generated */
  public boolean getLastBlock(int addr) {
    if (featOkTst && casFeat_lastBlock == null)
      jcas.throwFeatMissing("lastBlock", "org.apache.uima.ducc.Workitem");
    return ll_cas.ll_getBooleanValue(addr, casFeatCode_lastBlock);
  }

  /** @generated */
  public void setLastBlock(int addr, boolean v) {
    if (featOkTst && casFeat_lastBlock == null)
      jcas.throwFeatMissing("lastBlock", "org.apache.uima.ducc.Workitem");
    ll_cas.ll_setBooleanValue(addr, casFeatCode_lastBlock, v);
  }

  /**
   * initialize variables to correspond with Cas Type and Features
   * 
   * @generated
   */
  public Workitem_Type(JCas jcas, Type casType) {
    super(jcas, casType);
    casImpl.getFSClassRegistry().addGeneratorForType((TypeImpl) this.casType, getFSGenerator());

    casFeat_sendToLast = jcas.getRequiredFeatureDE(casType, "sendToLast", "uima.cas.Boolean",
            featOkTst);
    casFeatCode_sendToLast = (null == casFeat_sendToLast) ? JCas.INVALID_FEATURE_CODE
            : ((FeatureImpl) casFeat_sendToLast).getCode();

    casFeat_sendToAll = jcas.getRequiredFeatureDE(casType, "sendToAll", "uima.cas.Boolean",
            featOkTst);
    casFeatCode_sendToAll = (null == casFeat_sendToAll) ? JCas.INVALID_FEATURE_CODE
            : ((FeatureImpl) casFeat_sendToAll).getCode();

    casFeat_inputspec = jcas.getRequiredFeatureDE(casType, "inputspec", "uima.cas.String",
            featOkTst);
    casFeatCode_inputspec = (null == casFeat_inputspec) ? JCas.INVALID_FEATURE_CODE
            : ((FeatureImpl) casFeat_inputspec).getCode();

    casFeat_outputspec = jcas.getRequiredFeatureDE(casType, "outputspec", "uima.cas.String",
            featOkTst);
    casFeatCode_outputspec = (null == casFeat_outputspec) ? JCas.INVALID_FEATURE_CODE
            : ((FeatureImpl) casFeat_outputspec).getCode();

    casFeat_encoding = jcas.getRequiredFeatureDE(casType, "encoding", "uima.cas.String", featOkTst);
    casFeatCode_encoding = (null == casFeat_encoding) ? JCas.INVALID_FEATURE_CODE
            : ((FeatureImpl) casFeat_encoding).getCode();

    casFeat_language = jcas.getRequiredFeatureDE(casType, "language", "uima.cas.String", featOkTst);
    casFeatCode_language = (null == casFeat_language) ? JCas.INVALID_FEATURE_CODE
            : ((FeatureImpl) casFeat_language).getCode();

    casFeat_bytelength = jcas.getRequiredFeatureDE(casType, "bytelength", "uima.cas.Integer",
            featOkTst);
    casFeatCode_bytelength = (null == casFeat_bytelength) ? JCas.INVALID_FEATURE_CODE
            : ((FeatureImpl) casFeat_bytelength).getCode();

    casFeat_blockindex = jcas.getRequiredFeatureDE(casType, "blockindex", "uima.cas.Integer",
            featOkTst);
    casFeatCode_blockindex = (null == casFeat_blockindex) ? JCas.INVALID_FEATURE_CODE
            : ((FeatureImpl) casFeat_blockindex).getCode();

    casFeat_blocksize = jcas.getRequiredFeatureDE(casType, "blocksize", "uima.cas.Integer",
            featOkTst);
    casFeatCode_blocksize = (null == casFeat_blocksize) ? JCas.INVALID_FEATURE_CODE
            : ((FeatureImpl) casFeat_blocksize).getCode();

    casFeat_lastBlock = jcas.getRequiredFeatureDE(casType, "lastBlock", "uima.cas.Boolean",
            featOkTst);
    casFeatCode_lastBlock = (null == casFeat_lastBlock) ? JCas.INVALID_FEATURE_CODE
            : ((FeatureImpl) casFeat_lastBlock).getCode();

  }
}
