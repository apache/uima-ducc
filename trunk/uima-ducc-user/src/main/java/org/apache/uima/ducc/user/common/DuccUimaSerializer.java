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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javax.xml.parsers.FactoryConfigurationError;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.uima.UIMAFramework;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.util.Level;
import org.apache.uima.util.XMLSerializer;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;


public class DuccUimaSerializer {
	private static final String LOAD_EXTERNAL_DTD = "http://apache.org/xml/features/nonvalidating/load-external-dtd";
	private static final String EXTERNAL_GENERAL_ENTITIES = "http://xml.org/sax/features/external-general-entities";
	private static final String EXTERNAL_PARAMETER_ENTITIES = "http://xml.org/sax/features/external-parameter-entities";
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
  private void secureXmlReader(XMLReader xmlReader) {
	    try {
	        xmlReader.setFeature(EXTERNAL_GENERAL_ENTITIES, false);
	      } catch (SAXNotRecognizedException e) {
	        UIMAFramework.getLogger().log(Level.WARNING, 
	            "XMLReader didn't recognize feature " + EXTERNAL_GENERAL_ENTITIES);
	      } catch (SAXNotSupportedException e) {
	        UIMAFramework.getLogger().log(Level.WARNING, 
	            "XMLReader doesn't support feature " + EXTERNAL_GENERAL_ENTITIES);
	      }

	      try {
	        xmlReader.setFeature(EXTERNAL_PARAMETER_ENTITIES, false);
	      } catch (SAXNotRecognizedException e) {
	        UIMAFramework.getLogger().log(Level.WARNING, 
	            "XMLReader didn't recognize feature " + EXTERNAL_PARAMETER_ENTITIES);
	      } catch (SAXNotSupportedException e) {
	        UIMAFramework.getLogger().log(Level.WARNING, 
	            "XMLReader doesn't support feature " + EXTERNAL_PARAMETER_ENTITIES);
	      }

	      try {
	        xmlReader.setFeature(LOAD_EXTERNAL_DTD,false);
	      } catch (SAXNotRecognizedException e) {
	        UIMAFramework.getLogger().log(Level.WARNING, 
	            "XMLReader didn't recognized feature " + LOAD_EXTERNAL_DTD);
	      } catch (SAXNotSupportedException e) {
	        UIMAFramework.getLogger().log(Level.WARNING, 
	            "XMLReader doesn't support feature " + LOAD_EXTERNAL_DTD);
	      }

  }
  /** 
   * Utility method for deserializing a CAS from an XMI String
   * Does both processing of requests arriving to this service
   *   and responses returning to this service, or to a client. 
   */
  public void deserializeCasFromXmi(String anXmlStr, CAS aCAS)
          throws FactoryConfigurationError, ParserConfigurationException, SAXException, IOException {

	XMLReader xmlReader = XMLReaderFactory.createXMLReader();
	secureXmlReader(xmlReader);
    Reader reader = new StringReader(anXmlStr);
    XmiCasDeserializer deser = new XmiCasDeserializer(aCAS.getTypeSystem());
    ContentHandler handler = deser.getXmiCasHandler(aCAS);
    xmlReader.setContentHandler(handler);
    xmlReader.parse(new InputSource(reader));
  }
  
}
