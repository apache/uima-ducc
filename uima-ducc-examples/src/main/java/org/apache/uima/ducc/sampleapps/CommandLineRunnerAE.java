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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.util.Iterator;
import java.util.List;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_component.JCasAnnotator_ImplBase;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;

import com.google.common.collect.Lists;

public class CommandLineRunnerAE extends JCasAnnotator_ImplBase {

  private List<Process> processes = Lists.newLinkedList();
  private String[] envp;
  private String   lastStderrLine;

  @Override
  public void initialize(UimaContext aContext) throws ResourceInitializationException {
    super.initialize(aContext);    
    System.out.println("AE init");
    
    String cp  = System.getenv("DUCC_BATCH_CLASSPATH");
    String env = System.getenv("DUCC_BATCH_ENVIRONMENT");
    String user= System.getenv("USER");
    String home= System.getenv("HOME");
    String lang= System.getenv("LANG");
    String jobid=System.getenv("JobId");
    envp = (env+" CLASSPATH="+cp+" USER="+user+" HOME="+home+" LANG="+lang+" DUCC_JOBID="+jobid).split("\\s+");
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
    Runtime rt = Runtime.getRuntime();
    Process pr = rt.exec(command, envp);

    StdoutReader sr = new StdoutReader();
    sr.initialize(pr);
    (new Thread(sr)).start();
    processes.add(pr);
    try {
      Class<? extends Process> cl = pr.getClass();
      Field field;
      field = cl.getDeclaredField("pid");
      field.setAccessible(true);
      Object pidObject = field.get(pr);
      System.out.println("AE: started PID=" + (Integer) pidObject);
    } catch (NoSuchFieldException e1) {
      e1.printStackTrace();
    } catch (SecurityException e1) {
      e1.printStackTrace();
    } catch (IllegalArgumentException e) {
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    }

    StderrReader ser = new StderrReader();
    ser.initialize(pr);
    (new Thread(ser)).start();

    try {
      pr.waitFor();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    int exitValue = pr.exitValue();
    for (Iterator<Process> iter = processes.listIterator(); iter.hasNext(); ) {
      Process p = iter.next();
      if (p == pr) {
          iter.remove();
      }
    }
    if (exitValue != 0) {
      throw new RuntimeException("Exit code=" + exitValue +" stderr=" + lastStderrLine);
    }
  }
  
    class StdoutReader
    implements Runnable
  {
    private Process mypr; 
    public void initialize(Process pr) {
      mypr = pr;
    }

    @Override
    public void run() {
      String line;

      BufferedReader br = new BufferedReader(new InputStreamReader(mypr.getInputStream()));
      try {
        while( (line = br.readLine()) != null)
        {
//           if(line.equals("exit")) {
//             break;
//           }
           System.out.println(line);
        }
        br.close();
      } catch (IOException e) {
        // exit the loop and the thread terminates ?
      }
      
    }
  }
    
    class StderrReader
    implements Runnable
  {
    private Process mypr; 
    public void initialize(Process pr) {
      mypr = pr;
    }

    @Override
    public void run() {
//      String line;

      BufferedReader br = new BufferedReader(new InputStreamReader(mypr.getErrorStream()));
      try {
        while( (lastStderrLine = br.readLine()) != null)
        {
//           if(line.equals("exit")) {
//             break;
//           }
           System.out.println(lastStderrLine);
        }
        br.close();
      } catch (IOException e) {
        // exit the loop and the thread terminates ?
      }
      
    }
  }

}
