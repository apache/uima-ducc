/* Licensed to the Apache Software Foundation (ASF) under one
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

 package org.apache.uima.ducc.sampleapps;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_component.JCasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;

public class CommandLineRunnerAE extends JCasAnnotator_ImplBase {

  private String[] envp;

  @Override
  public void initialize(UimaContext aContext) throws ResourceInitializationException {
    super.initialize(aContext);    
    System.out.println("AE init");
    
    String cp  = System.getenv("DUCC_BATCH_CLASSPATH");
    String env = System.getenv("DUCC_BATCH_ENVIRONMENT");
    String user= System.getenv("USER");
    String home= System.getenv("HOME");
    String lang= System.getenv("LANG");
    String jobid=System.getenv("DUCC_JOBID");
    
    // Must not pass a null environment element to Runtime.exec
    if (env == null) {
      env = "";
    }
    envp = (env+" CLASSPATH="+cp+" USER="+user+" HOME="+home+" LANG="+lang+" DUCC_JOBID="+jobid).trim().split("\\s+");
  }
  
  @Override
  public void process(JCas jcas) throws AnalysisEngineProcessException {

    String command = jcas.getDocumentText();
    try {
      runCommand(command, envp);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void destroy() {
    super.destroy();
    System.out.println("CommandLineRunnerAE: Destroy method was called");
  }

  public void runCommand(String command, String[] envp) throws IOException {
    System.out.println("Running: " + command);
    
    // NOTE - Runtime.exec doesn't handle quoted arguments
    
    // Parse the cmd by matching double or single quoted strings or non-white-space sequences
    String regex = "\"([^\"]*)\"|'([^']*)'|(\\S+)";
    Matcher m = Pattern.compile(regex).matcher(command);
    ArrayList<String> args = new ArrayList<>();
    while (m.find()) {
      if (m.group(1) != null) {
        args.add(m.group(1));           // double-quoted
      } else if (m.group(2) != null) {
        args.add(m.group(2));           // single quoted
      } else {
        args.add(m.group(3));           // not quoted
      }
    }
    
    // Run the command with all output to stdout
    ProcessBuilder pb = new ProcessBuilder(args);
    pb.redirectErrorStream(true);
    pb.redirectOutput(ProcessBuilder.Redirect.INHERIT);
    Process pr = pb.start();    
    
    // Log the PID
    try {
      Class<? extends Process> cl = pr.getClass();
      Field field = cl.getDeclaredField("pid");
      field.setAccessible(true);
      System.out.println("AE: started PID=" + (Integer) field.get(pr));
    } catch (Exception e) {
      e.printStackTrace();
    }

    // Wait for the process to complete
    try {
      pr.waitFor();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    int exitValue = pr.exitValue();
    if (exitValue != 0) {
      throw new RuntimeException("Exit code=" + exitValue);
    }
  }

}
