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

package org.apache.uima.ducc.ws.server;

import java.io.File;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.Properties;

import org.apache.uima.ducc.cli.DuccUiConstants;
import org.apache.uima.ducc.common.utils.AlienFile;
import org.apache.uima.ducc.transport.event.common.IDuccWorkJob;
import org.apache.uima.ducc.ws.utils.alien.EffectiveUser;

public class DuccFile {

    /*
     * Returns null if file is missing or inaccessible
     */
	public static Properties getUserSpecifiedProperties(EffectiveUser eu, IDuccWorkJob job) throws Throwable {
		String directory = job.getUserLogDir();
		String name = DuccUiConstants.user_specified_properties;
		Properties properties = null;
		try {
			properties = DuccFile.getProperties(eu, directory+name);
		}
		catch(Exception e) {
			// no worries
		}
		return properties;
	}

	public static Properties getFileSpecifiedProperties(EffectiveUser eu, IDuccWorkJob job) throws Throwable {
		String directory = job.getUserLogDir();
		String name = DuccUiConstants.file_specified_properties;
		Properties properties = null;
		try {
			properties = DuccFile.getProperties(eu, directory+name);
		}
		catch(Exception e) {
			// no worries
		}
		return properties;
	}

	public static Properties getProperties(EffectiveUser eu, String path) throws Throwable {
		StringReader sr = null;
		try {
			AlienFile alienFile = new AlienFile(eu.get(), path);
			String data = alienFile.getString();
			if (data == null) {
				return null;
			}
			sr = new StringReader(data);
			Properties properties = new Properties();
			properties.load(sr);
			sr.close();
			return properties;
		}
		finally {
			try {
				if(sr != null) {
					sr.close();
				}
			}
			catch(Throwable t) {
			}
		}
	}

	public static InputStreamReader getInputStreamReader(EffectiveUser eu, String path) throws Throwable {
		AlienFile alienFile = new AlienFile(eu.get(), path);
		return alienFile.getInputStreamReader();
	}
}
