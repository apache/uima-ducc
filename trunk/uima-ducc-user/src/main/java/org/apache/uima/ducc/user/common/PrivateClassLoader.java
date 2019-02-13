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
package org.apache.uima.ducc.user.common;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;

/*
 * Create a URL class-loader from a classpath string or an array of classpath elements
 * as a peer (not a child) of the system class-loader.
 * Expand wild-cards to a list of jars
 * Quietly ignore missing files just as Java does
 * Resolve symbolic links as Java does (so Sicstus can find its native library!)
 * NOTE: directory elements in a URLClassLoader must end in a '/' but the toURI() method adds those 
 */

public class PrivateClassLoader {

  public static URLClassLoader create(String classPath) throws IOException {
    return create(classPath.split(":"));
  }

  public static URLClassLoader create(String[] classPathElements) throws IOException  {
    ArrayList<URL> urlList = new ArrayList<URL>(classPathElements.length);
    for (String element : classPathElements) {
      if (element.endsWith("*")) {
        File dir = new File(element.substring(0, element.length() - 1));
        File[] files = dir.listFiles();   // Will be null if missing or not a dir
        if (files != null) {
          for (File f : files) {
            if (f.getName().endsWith(".jar")) {
              urlList.add(f.getCanonicalFile().toURI().toURL());
            }
          }
        }
      } else {
        File f = new File(element);
        if (f.exists()) {
          urlList.add(f.getCanonicalFile().toURI().toURL());
        }
      }
    }
    URL[] urls = new URL[urlList.size()];
    return new URLClassLoader(urlList.toArray(urls), ClassLoader.getSystemClassLoader().getParent());
  }
  
  /* 
   * Dump all the URLs
   */
  public static void dump(ClassLoader cl, int numLevels) {
    int n = 0;
    for (URLClassLoader ucl = (URLClassLoader) cl; ucl != null && ++n <= numLevels; ucl = (URLClassLoader) ucl.getParent()) {
      System.out.println("Class-loader " + n + " has " + ucl.getURLs().length + " urls:");
      for (URL u : ucl.getURLs()) {
        System.out.println("  " + u );
      }
    }
  }
  
	public static void main(String[] args) throws IOException {
		if (args.length < 2) {
			System.out.println("args:  classpath class-to-load");
			return;
		}
		URLClassLoader pcl = create(args[0]);
		try {
			pcl.loadClass(args[1]);
			System.out.println("loadClass OK");
		} catch (ClassNotFoundException e) {
			System.out.println("loadClass failed");
		}

		try {
			Class.forName(args[1], false, pcl);
			System.out.println("forName OK");
		} catch (ClassNotFoundException e) {
			System.out.println("forName failed");
		}

		URL res = pcl.findResource(args[1]);
		if (res != null) {
			System.out.println("findResource: " + res);
		} else {
			System.out.println("findResource failed");
		}

		if (args.length > 2) {
			dump(pcl, 1);
		}
	}
}
