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
package org.apache.uima.ducc.user.jd.iface;

public class JdUserMetaCas {

	private int seqNo = -1;
	private String serializedCas = null;
	private String documentText = null;
	private Exception exception = null;
	
	public JdUserMetaCas(int seqNo, String serializedCas, String documentText) {
		setSeqNo(seqNo);
		setSerializedCas(serializedCas);
		setDocumentText(documentText);
	}
	
	public JdUserMetaCas(int seqNo, String serializedCas, String documentText, Exception exception) {
		setSeqNo(seqNo);
		setSerializedCas(serializedCas);
		setDocumentText(documentText);
		setException(exception);
	}
	
	private void setSeqNo(int value) {
		seqNo = value;
	}
	
	public int getSeqNo() {
		return seqNo;
	}
	
	private void setSerializedCas(String value) {
		serializedCas = value;
	}
	
	public String getSerializedCas() {
		return serializedCas;
	}
	
	private void setDocumentText(String value) {
		documentText = value;
	}
	
	public String getDocumentText() {
		return documentText;
	}
	
	private void setException(Exception value) {
		exception = value;
	}
	
	public Exception getException() {
		return exception;
	}
	
	public void printMe() {
		StringBuffer sb = new StringBuffer();
		sb.append("seq:"+getSeqNo()+" ");
		sb.append("id:"+getDocumentText()+" ");
		sb.append("cas:"+getSerializedCas()+" ");
		if(exception != null) {
			sb.append("exception:"+getException());
		}
		System.out.println(sb);
	}
}
