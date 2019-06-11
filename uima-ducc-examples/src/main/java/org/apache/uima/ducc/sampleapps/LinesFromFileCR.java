/* Licensed to the Apache Software Foundation (ASF) under one
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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;

import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.collection.CollectionReader_ImplBase;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Progress;
import org.apache.uima.util.ProgressImpl;

import com.google.common.collect.Lists;


public class LinesFromFileCR extends CollectionReader_ImplBase {

  public static final String PARAM_FILENAME = "Filename";
  private List<String> lines = Lists.newArrayList();
  private int lineCount = 0;
  private String filename;

  @Override
  public void initialize() throws ResourceInitializationException {
    System.out.println("CR: init");
    super.initialize();

    Object configParameterValue = getUimaContext().getConfigParameterValue(PARAM_FILENAME);
    if (configParameterValue == null) {
      throw new RuntimeException("Filename: config param not set");
    }
    filename = (String) configParameterValue;
    int linecnt = 0;
    try {
      FileReader fileReader = new FileReader(filename);
      BufferedReader bufferedReader = new BufferedReader(fileReader);
      String line = null;
      while ((line = bufferedReader.readLine()) != null) {
        if (! "#".equals(line.substring(0, 1))) {
          lines.add(line);
          linecnt++;
        }
      }
      bufferedReader.close();
      System.out.println("CR: found " + linecnt + " commands to run");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public synchronized void getNext(CAS cas) throws IOException, CollectionException {
    cas.reset();
    cas.setSofaDataString(lines.get(lineCount), "text");
    System.out.println("CR: " + lineCount + " = " + cas.getDocumentText());
    lineCount++;
    return;
  }

  @Override
  public boolean hasNext() throws IOException {
    return (lines.size() > lineCount);
  }

  @Override
  public Progress[] getProgress() {
    return new Progress[] { new ProgressImpl(lineCount, lines.size(), Progress.ENTITIES) };
  }

  @Override
  public void close() throws IOException {
  }

}
