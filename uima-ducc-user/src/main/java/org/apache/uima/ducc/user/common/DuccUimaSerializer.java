/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.uima.ducc.user.common;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;

import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.internal.util.XMLUtils;
import org.apache.uima.util.XMLSerializer;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

//import com.thoughtworks.xstream.XStream;
//import com.thoughtworks.xstream.io.xml.DomDriver;
//import java.util.concurrent.ConcurrentHashMap;

public class DuccUimaSerializer {

  /**
   * Utility method for serializing a CAS to an XMI String
   */
  public String serializeCasToXmi(CAS aCAS)
          throws Exception {
    Writer writer = new StringWriter();
    try {
      XMLSerializer xmlSer = new XMLSerializer(writer, false);
      XmiCasSerializer ser = new XmiCasSerializer(aCAS.getTypeSystem());
      ser.serialize(aCAS, xmlSer.getContentHandler());
      return writer.toString();
    } catch (SAXException e) {
      throw e;
    } finally {
      writer.close();
    }
  }

  /** 
   * Utility method for deserializing a CAS from an XMI String
   * Does both processing of requests arriving to this service
   *   and responses returning to this service, or to a client. 
   */
  public void deserializeCasFromXmi(String anXmlStr, CAS aCAS)
          throws FactoryConfigurationError, ParserConfigurationException, SAXException, IOException {

	XMLReader xmlReader =
		  XMLUtils.createXMLReader();
	  
    //XMLReader xmlReader = XMLReaderFactory.createXMLReader(); // localXmlReader.get();
    Reader reader = new StringReader(anXmlStr);
    XmiCasDeserializer deser = new XmiCasDeserializer(aCAS.getTypeSystem());
    ContentHandler handler = deser.getXmiCasHandler(aCAS);
    xmlReader.setContentHandler(handler);
    xmlReader.parse(new InputSource(reader));
  }
  
}
