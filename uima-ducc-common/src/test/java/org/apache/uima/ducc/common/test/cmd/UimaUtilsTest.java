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
package org.apache.uima.ducc.common.test.cmd;

import java.util.ArrayList;
import java.util.List;

import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.ducc.common.uima.UimaUtils;
import org.apache.uima.ducc.common.utils.Utils;
import org.apache.uima.resourceSpecifier.factory.UimaASPrimitiveDeploymentDescriptor;


public class UimaUtilsTest {

	public static void main(String[] args) {
		try {
			System.setProperty(UimaUtils.FlowControllerResourceSpecifier, "org.apache.uima.flow.FixedFlowController");
			String fname = "0-uima-ae-descriptor-"+Utils.getPID()+".xml";
			UimaASPrimitiveDeploymentDescriptor d = UimaUtils.createUimaASDeploymentDescriptor(
					"test", "testDescr", "tcp://localhost:61616",
					"testEndpoint", 4, "c:/tmp", fname, args);
			System.out.println(d.toXML());

			List<String> cmOverrides = new ArrayList<String>();
			List<String> aeOverrides = new ArrayList<String>();
			List<String> ccOverrides = new ArrayList<String>();
			ccOverrides.add("outputDir=/home/ducc");
			// aeOverrides.add("ErrorFrequency=4");
			// aeOverrides.add("ProcessDelay=4000");

			List<List<String>> overrides = new ArrayList<List<String>>();
			overrides.add(cmOverrides);
			overrides.add(aeOverrides);
			overrides.add(ccOverrides);

			AnalysisEngineDescription aed = UimaUtils
					.createAggregateDescription(true, overrides, args);
			aed.toXML(System.out);
			/*
			 * File tempAEDescriptorFile = null; File duccTempDir = new
			 * File("c:/tmp"); if ( !duccTempDir.exists()) {
			 * duccTempDir.mkdir(); } tempAEDescriptorFile =
			 * File.createTempFile( "UimaAEDescriptor", ".xml",duccTempDir);
			 * tempAEDescriptorFile.deleteOnExit(); FileOutputStream fos = new
			 * FileOutputStream(tempAEDescriptorFile); // Save xml descriptor
			 * into a default (OS specific) temp directory aed.toXML(fos);
			 * fos.close();
			 * 
			 * 
			 * ResourceSpecifier rs =
			 * UimaClassFactory.produceResourceSpecifier(tempAEDescriptorFile
			 * .getAbsolutePath()); UIMAFramework.produceAnalysisEngine(rs);
			 * 
			 * UimaASPrimitiveDeploymentDescriptor dd = UimaUtils
			 * .createUimaASDeploymentDescriptor("Test", "Test Description",
			 * "tcp://localhost:61616", "TestQueue", 2,
			 * Utils.resolvePlaceholderIfExists("${DUCC_HOME}/tmp",
			 * System.getProperties()),args); System.out.println(dd.toXML());
			 * File tempddDescriptorFile = null; tempddDescriptorFile =
			 * File.createTempFile( "UimaASDeploymentDescriptor", ".xml");
			 * tempddDescriptorFile.deleteOnExit();
			 * 
			 * BufferedWriter out = new BufferedWriter(new
			 * FileWriter(tempddDescriptorFile)); out.write(dd.toXML());
			 * out.close();
			 */
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
