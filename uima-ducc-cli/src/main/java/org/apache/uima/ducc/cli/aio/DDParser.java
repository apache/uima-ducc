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
package org.apache.uima.ducc.cli.aio;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.uima.UIMAFramework;
import org.apache.uima.util.Level;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.SAXNotRecognizedException;
import org.xml.sax.SAXNotSupportedException;
import org.xml.sax.helpers.DefaultHandler;

public class DDParser extends DefaultHandler {
	
	private static final String DISALLOW_DOCTYPE_DECL = "http://apache.org/xml/features/disallow-doctype-decl";
	private static final String LOAD_EXTERNAL_DTD = "http://apache.org/xml/features/nonvalidating/load-external-dtd";

	private File file = null;
	private FileInputStream fis = null;
	private SAXParser parser = null;
	
	private String ddImport = null;
	
	public DDParser(String filename) throws ParserConfigurationException, SAXException, IOException {
		file = new File(filename);
		parse();
	}
	
	public DDParser(File file) throws ParserConfigurationException, SAXException, IOException {
		this.file = file;
		parse();
	}
	private void secureFactory(SAXParserFactory f) {
        try {
            f.setFeature(DISALLOW_DOCTYPE_DECL, true);
          } catch (SAXNotRecognizedException e) {
            UIMAFramework.getLogger().log(Level.WARNING, 
                "SAXParserFactory didn't recognize feature " + DISALLOW_DOCTYPE_DECL);
          } catch (SAXNotSupportedException e) {
            UIMAFramework.getLogger().log(Level.WARNING, 
                "SAXParserFactory doesn't support feature " + DISALLOW_DOCTYPE_DECL);
          } catch (ParserConfigurationException e) {
            UIMAFramework.getLogger().log(Level.WARNING, 
                "SAXParserFactory doesn't support feature " + DISALLOW_DOCTYPE_DECL);
          }
          
          try {
            f.setFeature(LOAD_EXTERNAL_DTD, false);
          } catch (SAXNotRecognizedException e) {
            UIMAFramework.getLogger().log(Level.WARNING, 
                "SAXParserFactory didn't recognize feature " + LOAD_EXTERNAL_DTD);
          } catch (SAXNotSupportedException e) {
            UIMAFramework.getLogger().log(Level.WARNING, 
                "SAXParserFactory doesn't support feature " + LOAD_EXTERNAL_DTD);
          } catch (ParserConfigurationException e) {
            UIMAFramework.getLogger().log(Level.WARNING, 
                "SAXParserFactory doesn't support feature " + LOAD_EXTERNAL_DTD);
          }
          f.setXIncludeAware(false);
	}
	private void parse() throws ParserConfigurationException, SAXException, IOException {
		fis = new FileInputStream(file);

		SAXParserFactory f = SAXParserFactory.newInstance();
    	secureFactory(f);
        parser = f.newSAXParser();
		parser.parse(fis, this);
	}
	
	@Override
	public void startDocument() throws SAXException {
		//System.out.println("startDocument");
	}
	
	@Override
	public void endDocument() throws SAXException {
		//System.out.println("endDocument");
	}
	
	@Override
	public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
		//System.out.println("qName: "+qName);
		if(qName != null) {
			if(qName.trim().equalsIgnoreCase("import")) {
				int count = attributes.getLength();
				for(int i = 0; i < count; i++) {
					String name = attributes.getQName(i);
					String value = attributes.getValue(i);
					if(name != null) {
						if(name.trim().equalsIgnoreCase("name") || name.trim().equalsIgnoreCase("location")) {
							ddImport = value;
						}
					}
				}
			}
		}
		
	}
	
	public String getDDImport() {
		return ddImport;
	}
	
	public static void main(String[] args) throws Exception {
		System.out.println(args[0]);
		DDParser ddParser = new DDParser(args[0]);
		System.out.println(ddParser.getDDImport());
	}

}
