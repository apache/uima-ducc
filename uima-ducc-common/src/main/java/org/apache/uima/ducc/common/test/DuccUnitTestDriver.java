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
package org.apache.uima.ducc.common.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.uima.UIMAFramework;
import org.apache.uima.UIMARuntimeException;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineManagement;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionReader;
import org.apache.uima.ducc.common.uima.InvalidOverrideParameterException;
import org.apache.uima.ducc.common.uima.UimaUtils;
import org.apache.uima.resource.ResourceCreationSpecifier;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.ConfigurationParameter;
import org.apache.uima.resource.metadata.ConfigurationParameterDeclarations;
import org.apache.uima.resource.metadata.ConfigurationParameterSettings;
import org.apache.uima.util.Progress;
import org.apache.uima.util.XMLInputSource;


public class DuccUnitTestDriver {
  private String crDesc = null;
  private List<String> crOverrides = new ArrayList<String>();
  private List<String> cmOverrides = new ArrayList<String>();
  private List<String> aeOverrides = new ArrayList<String>();
  private List<String> ccOverrides = new ArrayList<String>();
  private String cmDesc = null;
  private String aeDesc = null;
  private String ccDesc = null;
  private String ddDesc = null;
  boolean verbose = false;


  public void RunJobComponents(String[] args) {

    try {
      if (!processCmdLineArgs(args)) {
        printUsageMessage();
        return;
      }

      File tempAEDescriptorFile = null;
      if (aeDesc != null) {
        if (cmDesc == null) {
          System.out.println("No CM descriptor specified.\n"+
                  "For Blade equivalent function use com.ibm.bluej.core.xmi_zip_reader.DuccXmiZipReaderCM");
        }
        if (ccDesc == null) {
          System.out.println("No CC descriptor specified.\n"+
                  "For Blade equivalent function use com.ibm.bluej.core.xmi_zip_writer.BlueJXmiZipWriter");
        }

        // create AE descriptor
        List<List<String>> overrides = new ArrayList<List<String>>();
        if (cmOverrides != null)
          overrides.add(cmOverrides);
        if (aeOverrides != null)
          overrides.add(aeOverrides);
        if (ccOverrides != null)
          overrides.add(ccOverrides);
        AnalysisEngineDescription aed = UimaUtils.createAggregateDescription(false, overrides, cmDesc,
                aeDesc, ccDesc);

        // and save it in /tmp directory
        File duccTempDir = new File("/tmp");
        tempAEDescriptorFile = File.createTempFile("UimaAEDescriptor", ".xml", duccTempDir);
        tempAEDescriptorFile.deleteOnExit();
        FileOutputStream fos = new FileOutputStream(tempAEDescriptorFile);
        aed.toXML(fos);
        aed.toXML(System.out);
        fos.close();
      }
      else {
        // find and parse the dd-aggregate descriptor
        if (ddDesc.endsWith(".xml")) {
          tempAEDescriptorFile = new File(ddDesc);
        }
        else {
          String relativePath = ddDesc.replace('.', '/')+".xml";
          URL url = getClass().getClassLoader().getResource(relativePath);
          if (url == null) {
            System.err.println(relativePath+" not found in classpath");
            System.exit(1);
          }
          tempAEDescriptorFile = new File(url.getFile());
        }
      }

      // find and parse the CR descriptor
      File crDescFile = null;
      if (crDesc.endsWith(".xml")) {
        crDescFile = new File(crDesc);
      }
      else {
        String relativePath = crDesc.replace('.', '/')+".xml";
        URL url = getClass().getClassLoader().getResource(relativePath);
        if (url == null) {
          System.err.println(relativePath+" not found in classpath");
          System.exit(1);
        }
        crDescFile = new File(url.getFile());
      }
      XMLInputSource in = new XMLInputSource(crDescFile);
      ResourceSpecifier specifier = UIMAFramework.getXMLParser().parseResourceSpecifier(in);

      // apply CR overrides, if any
      if (!crOverrides.isEmpty()) {
        ConfigurationParameterSettings cps = ((ResourceCreationSpecifier) specifier).getMetaData()
            .getConfigurationParameterSettings();
        for (String cmOverride : crOverrides) {
          System.out.println(".... Processing Override:"+cmOverride);
          // each override is expressed as <name>=<value> pair
          String[] nvp = cmOverride.split("=",2);
          ConfigurationParameterDeclarations componentParameterDeclarations = ((ResourceCreationSpecifier) specifier)
              .getMetaData().getConfigurationParameterDeclarations();
          boolean found = false;
          for (ConfigurationParameter parameter : componentParameterDeclarations
              .getConfigurationParameters()) {
            if (nvp[0].equals(parameter.getName())) {
              if (ConfigurationParameter.TYPE_STRING.equals(parameter.getType())) {
                cps.setParameterValue(nvp[0], nvp[1]);
              }
              else if (ConfigurationParameter.TYPE_FLOAT.equals(parameter.getType())) {
                cps.setParameterValue(nvp[0], new Float(nvp[1]));
              }
              else if (ConfigurationParameter.TYPE_INTEGER.equals(parameter.getType())) {
                cps.setParameterValue(nvp[0], new Integer(nvp[1]));
              }
              else { //assume boolean
                cps.setParameterValue(nvp[0], new Boolean(nvp[1]));
              }
              found = true;
              break;
            }
          }
          if (!found) {
            throw new UIMARuntimeException(
                new InvalidOverrideParameterException(
                    "Override Parameter:"
                        + nvp[0]
                        + " is not defined for the specified collection reader: "
                        + crDesc));
          }
        }
      }

      // instantiate the CR and AE
      CollectionReader cr = UIMAFramework.produceCollectionReader(specifier);
      in = new XMLInputSource(tempAEDescriptorFile);
      specifier = UIMAFramework.getXMLParser().parseResourceSpecifier(in);
      AnalysisEngine ae = UIMAFramework.produceAnalysisEngine(specifier);

      //get the AE management objects that collect timing stats for each component
      List<AnalysisEngineManagement> aeManagementObjects = new ArrayList<AnalysisEngineManagement>();
      AnalysisEngineManagement rootAem = ae.getManagementInterface();
      aeManagementObjects.add(rootAem);    
      getLeafManagementObjects(rootAem, aeManagementObjects);
      rootAem.resetStats();

      // only one CAS needed for this single threaded process
      CAS cas = ae.newCAS();
      
      Progress[] prog = cr.getProgress();
      if (prog != null) {
        long total = prog[0].getTotal();
        System.out.println("\n"+total+" work items specified by collection reader\n");
      }
      else {
        System.err.println("Warning: collection reader does not specify number of work items");
      }

      // process all work items
      int numProc=0;
      while (cr.hasNext()) {
        cr.getNext(cas);
        ae.process(cas);
        cas.reset();
        numProc++;
        if (verbose){
          System.out.print(".");
          if (0 == numProc%100) {
            System.out.println();
          }
        }
      }
      if (verbose){
        System.out.println("\nProcessed "+numProc+" work items");
        printStats(System.out, ae);
      }
      
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private void printUsageMessage() {
    System.err.println("\nUnit test driver for components to be deployed by Ducc\n"
            +" This test driver allows single threaded testing of a job collection reader\n"
            +" plus a job process\n"
            +"   assembled from [CM] + AE + [CC] descriptors, or\n"
            +"   specified by an AE descriptor intended to be used via a Deployment Descriptor\n"
            +"\nUsage:\n java " + this.getClass().getName()
            +" --driver_descriptor_CR <desc>\n"
            +" [--driver_descriptor_CR_overrides <list>]\n"
            +" ...and then either...\n"
            +" --process_descriptor_AE <desc>\n"
            +" [--process_descriptor_AE_overrides <list>]\n" 
            +" [--process_descriptor_CM <desc>]\n"
            +" [--process_descriptor_CM_overrides <list>]\n" 
            +" [--process_descriptor_CC <desc>]\n"
            +" [--process_descriptor_CC_overrides <list>]\n"
            +" ...or...\n"
            +" --process_descriptor_4DD <desc>\n"
            +" [-verbose]\n");
  }
  
  private ArrayList<String> toArrayList(String overrides) {
    ArrayList<String> list = new ArrayList<String>();
    if(overrides != null) {
      String[] items = overrides.split(",");
      for(String item : items) {
        list.add(item.trim());
      }
    }
    return list;
  }

  private boolean processCmdLineArgs(String[] args) {
    int index = 0;
    if (args.length < 1) {
      return false;
    }
    
    while (index < args.length) {
      String arg = args[index++];
      if (arg.equals("--process_descriptor_AE")) {
        aeDesc = args[index++];
      } else if (arg.equals("--driver_descriptor_CR")) {
        crDesc = args[index++];
      } else if (arg.equals("--process_descriptor_CM")) {
        cmDesc = args[index++];
      } else if (arg.equals("--process_descriptor_CC")) {
        ccDesc = args[index++];
      } else if (arg.equals("--process_descriptor_4DD")) {
        ddDesc = args[index++];
      } else if (arg.equals("--driver_descriptor_CR_overrides")) {
        crOverrides = toArrayList(args[index++]);
      } else if (arg.equals("--process_descriptor_AE_overrides")) {
        aeOverrides = toArrayList(args[index++]);
      } else if (arg.equals("--process_descriptor_CM_overrides")) {
        cmOverrides = toArrayList(args[index++]);
      } else if (arg.equals("--process_descriptor_CC_overrides")) {
        ccOverrides = toArrayList(args[index++]);
      } else if (arg.equals("-verbose")) {
        verbose=true;
      } else {
        System.out.println("Ignoring arg = "+args[index++]);
      }
    }
    if (crDesc == null) {
      System.err.println("\nNo CR descriptor specified.\n"+
              "For Blade equivalent function use com.ibm.bluej.core.ducc.DuccXmiZipJobDriverCR");
      return false;
    }
    if (aeDesc == null && ddDesc == null) {
      System.err.println("\nNo AE or DD descriptor specified.");
      return false;
    }
    if (ddDesc != null && (aeDesc != null || cmDesc != null || ccDesc != null)) {
      System.err.println("\n4DD descriptor specified as well as one or more of {AE,CM,CC} descriptors, will not continue.");
      return false;
    }
    return true;
  }

  public static void main(String[] args) {
    DuccUnitTestDriver td = new DuccUnitTestDriver();
    try {
      td.RunJobComponents(args);
    } catch (Throwable t) {
      System.err.println("Ducc unit test driver caught exception:");
      t.printStackTrace();
    }
  }

  private static void getLeafManagementObjects(AnalysisEngineManagement aem, List<AnalysisEngineManagement> result) {
    if (aem.getComponents().isEmpty()) {
      if (!aem.getName().equals("Fixed Flow Controller")) {
        result.add(aem);
      }
    } else {
      for (AnalysisEngineManagement child : (Iterable<AnalysisEngineManagement>) aem.getComponents().values()) {
        getLeafManagementObjects(child, result);
      }
    }
  }

  private static void printStats(PrintStream out, AnalysisEngine userAE ) {
    AnalysisEngineManagement userAEM = userAE.getManagementInterface();

    printComponentStats(out, 0, userAEM);
  }

  private static void printComponentStats(PrintStream out, int level, AnalysisEngineManagement aem) {
    String indent = "";
    for (int i = 0; i < level; i++)
      indent += "  ";
    out.println(indent + aem.getName() + ": " + aem.getAnalysisTime() + "ms, ");
    for (AnalysisEngineManagement childAem : (Iterable<AnalysisEngineManagement>) (aem
            .getComponents().values()))
      printComponentStats(out, level + 1, childAem);

  }

}
