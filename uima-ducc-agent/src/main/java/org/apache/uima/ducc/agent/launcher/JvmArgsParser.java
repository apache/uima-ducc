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
package org.apache.uima.ducc.agent.launcher;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses jvm args String provided by the user and returns a List of jvm args. The parser only
 * expects -X and -D in the jvm args String. Supports a mix of -X and -D args in an arbitrary order.
 *
 */
public class JvmArgsParser {

  /**
   * Parse -X args
   * 
   * @param cmd2run
   *          - where to add -X args
   * @param arg
   *          - string containing zero or more -X args
   */
  private static void addJvmOptions(List<String> cmd2run, String arg) {
    if (arg == null || arg.trim().length() == 0) {
      return;
    }
    boolean ignoreFirstArg = false;
    // the arg string may start with non -X arg, in which case ignore
    // that first part.
    if (!arg.trim().startsWith("-")) {
      ignoreFirstArg = true;
    }
    // Get -X tokens
    String[] jvm_args = arg.split("-X");
    for (String jvm_arg : jvm_args) {
      // check if the first token contains non -X part, in which case ignore it
      if (ignoreFirstArg) {
        ignoreFirstArg = false;
        continue;
      }
      // check if this is valid -X token and add it to a given list
      if (jvm_arg.trim().length() > 0) {
        cmd2run.add("-X" + jvm_arg.trim()); // -X was stripped by split()
      }
    }
  }

  /**
   * Return a List of jvm args contained in a provided args string. It first tokenizes the args
   * string extracting -D args. Secondly, it parses each token and extracts -X args.
   * 
   * @param args
   *          - string containing a mix of -X and -D args
   * 
   * @return List<String> - list of jvm args
   */
  public static List<String> parse(String args) {
    List<String> jvm_args = new ArrayList<String>();
    // tokenize on -D boundaries. Produced tokens may contain both -D and -X args
    String[] jvm_args_array = args.split("-D");
    for (String arg : jvm_args_array) {
      if (arg.trim().length() > 0) {
        int start = 0;
        arg = arg.trim();
        // check if the current token contains -X arg
        if ((start = arg.indexOf("-X")) > -1) {
          // parse current token and add -X args to the list
          addJvmOptions(jvm_args, arg);
        } else {
          jvm_args.add("-D" + arg); // -D was stripped by split()
          continue;
        }
        if (start == 0) {
          continue;
        }
        jvm_args.add("-D" + arg.substring(0, start).trim()); // -D was stripped by split()
      }
    }
    return jvm_args;
  }
}
