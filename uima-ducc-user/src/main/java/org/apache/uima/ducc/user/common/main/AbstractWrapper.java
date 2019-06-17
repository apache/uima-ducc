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
package org.apache.uima.ducc.user.common.main;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

import javax.xml.parsers.FactoryConfigurationError;

import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.internal.util.XMLUtils;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

public class AbstractWrapper {

	protected void deserializeCasFromXmi(String anXmlStr, CAS aCAS)
			throws FactoryConfigurationError, SAXException, IOException {

		Reader reader = new StringReader(anXmlStr);
		XMLReader xmlReader = XMLUtils.createXMLReader();
		XmiCasDeserializer deser = new XmiCasDeserializer(aCAS.getTypeSystem());
		ContentHandler handler = deser.getXmiCasHandler(aCAS);
		xmlReader.setContentHandler(handler);
		xmlReader.parse(new InputSource(reader));
	}


}
