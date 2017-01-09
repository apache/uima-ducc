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
package org.apache.uima.ducc.transport.event.common.history;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

import org.apache.uima.ducc.common.main.DuccService;
import org.apache.uima.ducc.common.utils.DuccLogger;

public class DeserializerObjectInputStream extends ObjectInputStream {

	private DuccLogger logger = DuccService
			.getDuccLogger(DeserializerObjectInputStream.class.getName());

	protected DeserializerObjectInputStream(InputStream is) throws IOException,
			SecurityException {
		super(is);
	}

	protected ObjectStreamClass readClassDescriptor() throws IOException,
			ClassNotFoundException {
		String location = "readClassDescriptor";
		ObjectStreamClass resultClassDescriptor = super.readClassDescriptor(); // initially streams descriptor
		@SuppressWarnings("rawtypes")
		Class localClass; // the class in the local JVM that this descriptor represents.
		try {
			localClass = Class.forName(resultClassDescriptor.getName());
		} catch (ClassNotFoundException e) {
			String text = "No local class for " + resultClassDescriptor.getName();
			logger.error(location, null, text, e);
			return resultClassDescriptor;
		}
		ObjectStreamClass localClassDescriptor = ObjectStreamClass.lookup(localClass);
		if (localClassDescriptor != null) { // only if class implements serializable
			final long localSUID = localClassDescriptor.getSerialVersionUID();
			final long streamSUID = resultClassDescriptor.getSerialVersionUID();
			if (streamSUID != localSUID) { // check for serialVersionUID mismatch.
				final StringBuffer sb = new StringBuffer("Overriding serialized class version mismatch: ");
				sb.append("local serialVersionUID = ").append(localSUID);
				sb.append(" stream serialVersionUID = ").append(streamSUID);
				logger.trace(location, null, sb);
				resultClassDescriptor = localClassDescriptor; // Use local class descriptor for deserialization
			}
		}
		return resultClassDescriptor;
	}
}
