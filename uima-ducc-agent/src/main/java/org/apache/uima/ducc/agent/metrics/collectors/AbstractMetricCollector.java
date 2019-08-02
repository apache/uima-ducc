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
package org.apache.uima.ducc.agent.metrics.collectors;

import java.io.IOException;
import java.io.RandomAccessFile;

public abstract class AbstractMetricCollector implements MetricCollector {
  private int howManyFields;

  protected int[] metricFieldOffsets;

  protected int[] metricFieldLengths;

  protected byte[] metricFileContents = new byte[1024];

  protected RandomAccessFile metricFile;

  private int mField;

  private int fieldOffset;

  private int currentOffset = 0;

  public AbstractMetricCollector(RandomAccessFile metricFile, int howMany, int offset) {
    this.howManyFields = howMany;
    this.fieldOffset = offset;
    this.metricFile = metricFile;
    metricFieldOffsets = new int[howMany];
    metricFieldLengths = new int[howMany];
  }

  public void parseMetricFile() throws IOException {
    metricFile.seek(0);
    metricFile.read(metricFileContents);
    if (fieldOffset > 0) {
      // Advance the pointer just beyond the field name
      while (metricFileContents[currentOffset] != ' ') {
        ++currentOffset;
      }
    }
    int currentFieldIndx = 0;
    while (currentFieldIndx++ < howManyFields) {
      readNextField();
    }
  }

  private void readNextField() {
    // Skip column padding.
    while (metricFileContents[currentOffset] == ' ') {
      ++currentOffset;
    }
    // Find the end of the value.
    int offset = currentOffset;
    while (metricFileContents[currentOffset] != ' ' && metricFileContents[currentOffset] != 0) {
      ++currentOffset;
    }
    // Store the value's offset and length.
    metricFieldOffsets[mField] = offset;
    metricFieldLengths[mField++] = currentOffset - offset;
    // Skip to the next value.
    currentOffset += fieldOffset;
  }
}
