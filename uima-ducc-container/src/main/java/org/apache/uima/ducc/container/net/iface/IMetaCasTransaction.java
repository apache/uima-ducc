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
package org.apache.uima.ducc.container.net.iface;

import java.io.Serializable;

import org.apache.uima.ducc.container.net.impl.TransactionId;

public interface IMetaCasTransaction extends IMetaCasProvider, IMetaCasRequester, Serializable {

	public enum Type { Get, Ack, End };
	
	public Type getType();
	public void setType(Type value);
	
	public enum Direction { Request, Response };
	
	public Direction getDirection();
	public void setDirection(Direction value);
	
	public TransactionId getTransactionId();
	public void setTransactionId(TransactionId value);

	public IMetaCas getMetaCas();
	public void setMetaCas(IMetaCas value);
	
	/*
	 * Initializing - driver is not yet ready to deliver work items
	 * Active - driver is ready or delivering work items
	 * Ended - driver is finished delivering work items
	 */
	public enum JdState { Initializing, Active, Ended };
	
	public JdState getJdState();
	public void setJdState(JdState value);
}
