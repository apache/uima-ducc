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
package org.apache.uima.ducc.common.node.metrics;

import java.io.Serializable;
import java.nio.ByteBuffer;

public abstract class ByteBufferParser implements Serializable {
  private static final long serialVersionUID = 5187826597226614457L;
  private int[] fieldOffsets;
  private int[] fieldLengths;
  private byte[] byteBuffer;

  public ByteBufferParser(byte[] byteBuffer, int[] fieldOffsets, int[] fieldLengths ) {
    this.byteBuffer = byteBuffer;
    this.fieldOffsets = fieldOffsets;
    this.fieldLengths = fieldLengths;

  }
  protected byte[] getFieldAsByteArray(int fieldIndex) {
    byte[] b= new byte[fieldLengths[fieldIndex]];
    ByteBuffer bb = ByteBuffer.wrap(byteBuffer);
    bb.position(fieldOffsets[fieldIndex]);
    bb.get(b, 0, fieldLengths[fieldIndex] );
    return b;
  }
  protected String getFieldAsString(int fieldIndex) {
    return new String(getFieldAsByteArray(fieldIndex));
  }
  protected Long getFieldAsLong(int fieldIndex) {
	    return Long.parseLong(new String(getFieldAsByteArray(fieldIndex)));
	  }

}
