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
package org.apache.uima.ducc.transport.dispatcher;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;

public class ClassManager
{
    private boolean DEBUG = false;
    private String DUCC_HOME = null;
    private URL[] urls = null;
    private ClassLoader classloader = null;

    private String[] private_classpath;

    public ClassManager(String[] cp)
        throws Exception
    {
        DUCC_HOME = System.getProperty("DUCC_HOME");
        if ( DUCC_HOME == null ) {
            throw new IllegalStateException("Internal error: DUCC_HOME must be set as a system property.");
        }

        this.private_classpath = cp;
        make_urls();
        HttpClassLoader top = new HttpClassLoader(urls, ClassLoader.getSystemClassLoader().getParent());
        classloader = new PrivateClassLoader(this.getClass().getClassLoader(), top);
        Thread.currentThread().setContextClassLoader(classloader);
        if ( DEBUG ) System.out.println("Context class loader set to " + classloader);
    }

    private void expand_wildcards(List<URL> in, String cp_entry)
    	throws Exception    	
    {
    	//
    	// We implement simple * wild-card
    	// - list the enclosing directory
    	// - get list of things matching PATH*
    	// - if thing is directory, add all its contents to the returned list of paths
    	// - if thing is file, add single entry to the list
    	// - if not found, throw, because it is an internal DUCC error to look for stuff that
    	//   can't exist
    	//
    	
        // First, everything in cp MUST be inside DUCC_HOME
        // TODO: Logic to get DUCC_HOME needs to be copied into this code in order to bypass
        //       dependencies on other DUCC libraries in the public part of the CLI.

        String path = DUCC_HOME + "/" + cp_entry;
        if ( ! path.endsWith("*") ) {
            File f = new File(path);
            if ( ! f.exists() ) {
                throw new IllegalStateException("Internal error: cannot find private classpath entry " + path);
            }

            if ( f.isFile() ) {
                // System.out.println("FILE: Adding " + f + " to generated classpath.");
                in.add(f.toURI().toURL());
            } else  if ( f.isDirectory() ) {
                if ( !path.endsWith("/") ) path = path + "/";
                in.add(f.toURI().toURL());
            }
            return;
        }

        cp_entry = cp_entry.substring(0, cp_entry.length()-1);
        int ndx = path.lastIndexOf("/");
        String p = path.substring(0, ndx);
        File parent = new File(p);
        if ( parent.isDirectory() ) {
            File[] contents = parent.listFiles();
            for (File f : contents ) {
                // System.out.println("Found file " + f);

                // If it's a directory then skip it, these need explicit mention
                // Otherwise try a 'startswith' match and if it matches, add to the returned list
                
                if ( f.isDirectory() ) continue;

                if ( f.toString().contains(cp_entry)) {
                    // System.out.println("Adding " + f + " to generated classpath.");
                    in.add(f.toURI().toURL());
                    continue;
                } else {
                	// System.out.println("Skipping cp entry " + cp_entry);
                }
            }
        } else {
            throw new Exception("Internal error: cannot find private classpath directory " + parent);
        }
        // System.exit(0);
    }

    private void make_urls()
    	throws Exception
    {
        
        List<URL> urlList = new ArrayList<URL>();
        
        for ( String elem : private_classpath ) {
            expand_wildcards(urlList, elem);
        }
        
        urls = urlList.toArray(new URL[urlList.size()]);
    }

    private int level = 0;
	private void listLoaders(ClassLoader cl)
    {
        // Debug only, it's only unused sometimes
        System.out.println("Loader at level " + level + " is " + cl);
        if ( cl.getParent() != null ) {
            level++;
            listLoaders(cl.getParent());
        }
    }

    public Class<?> loadClass(String name)
    	throws MalformedURLException,
    	ClassNotFoundException    
    {        
        if ( DEBUG ) {
            System.out.println("app_loader");
            level = 0;
            listLoaders(classloader);
        }

        // This should prevent the usual "default" classloader with the users gorp from sneaking in via
        // classloaders in the stuff we bring in, such as log4j
        Thread.currentThread().setContextClassLoader(classloader);

        if ( DEBUG ) {            
            System.out.println("app_loader is " + classloader);
            System.out.println("My loader is " + getClass().getClassLoader());
            System.out.println("System class loader is " + ClassLoader.getSystemClassLoader());
            level = 0;
            listLoaders(classloader);
        }

        // return app_loader.loadClass(name);
        return Class.forName(name, true, classloader);
    }


    private boolean argumentsMatch(Class<?>[] cparms, Object[] args)
    {
        if ( cparms.length != args.length ) return false;

        for ( int i = 0; i < cparms.length; i++ ) {
        	Class<?> cl = null;
        	if ( args[i] == null ) {
        		cl = null;
        	} else {
                cl = args[i].getClass();
        	}
        	
        	if ( !cparms[i].isPrimitive() && (cl == null) ) continue; // we'll assume everything we pass in now is assignable to null
        	
            if ( !cparms[i].isAssignableFrom(cl) ) return false;

            Class<?>[] declared = cl.getClasses();
            for ( int j = 0; j < declared.length; j++ ) {
                if ( DEBUG ) System.out.println("Check argument match cparms: " + cparms[i] + " with " + declared[j]);
                if ( cparms[i].isAssignableFrom(declared[j]) ) return true;
            }                                                
        }
        return true;
    }


    /**
     * Constructor with arguments.
     */
	public Object construct(String name, Object[] args)
    	throws Exception
    {
        Class<?> cls = loadClass(name);

        if ( DEBUG ) System.out.println("Context class loader: " + Thread.currentThread().getContextClassLoader());

        Constructor<?> cons = null;
        Constructor<?>[] allConstructors = cls.getConstructors();
        for ( Constructor<?> c : allConstructors ) {
            if ( DEBUG ) System.out.println("Constructor for " + name + " : " + c);
            Class<?>[] cparms = c.getParameterTypes();

            if ( DEBUG ) {
                for ( Class<?> cc : cparms ) {
                    System.out.println("   takes parameter " + cc.getName() + " : from " + cc.getClassLoader());
                }
            }

            if ( cparms.length != args.length ) {
                if ( DEBUG ) System.out.println("Constructor " + c + " is not viable because argument length differs.");
                continue;

            }
            if ( argumentsMatch(cparms, args) ) {
                cons = c;
                break;
            }

        }
        if ( cons == null ) {
            throw new ClassNotFoundException(name + " has no compatible arguments.");
        } 
        if ( DEBUG ) System.out.println("Use constructor " + cons);
        
        Object o = cons.newInstance(args);
        return o;
    }

    /**
     * No args constructor
     */
    public Object construct(String name)
    	throws Exception
    {
        Class<?> cls = loadClass(name);

        if ( DEBUG ) System.out.println("Context class loader: " + Thread.currentThread().getContextClassLoader());
        
        return cls.newInstance();
    }

    public Object invoke(Object obj, String meth) 
        throws Exception
    {
        return invoke(obj, meth, null);
    }

    public Object invoke(Object obj, String meth, Object[] args) 
        throws Exception
    {
        Method m = null;
        if ( args != null ) {
            Method[] methods = obj.getClass().getMethods();
            for ( Method mm : methods ) {
                if ( !mm.getName().equals(meth) ) continue;

                Class<?>[] cparms = mm.getParameterTypes();
                if ( argumentsMatch(cparms, args ) ) {
                	m = mm;
                	break;
                }
            }

            if ( m == null ) throw new NoSuchMethodException(meth);
            // Class<?>[] cl_args= new Class[args.length];
            // for ( int i = 0; i < args.length; i++ ) {
            //     cl_args[i] = args[i].getClass();
            // } 
            // m = obj.getClass().getMethod(meth, cl_args);
        } else {
            m = obj.getClass().getMethod(meth);
        }

        Object ret = m.invoke(obj, args);
        return ret;
    }

    public Object invokex(Object obj, String meth, Object[] args) 
        throws Exception
    {
        Method m;
        if ( args != null ) {
            Class<?>[] cl_args= new Class[args.length];
            for ( int i = 0; i < args.length; i++ ) {
                cl_args[i] = args[i].getClass();
            } 
            m = obj.getClass().getMethod(meth, cl_args);
        } else {
            m = obj.getClass().getMethod(meth);
        }

        Object ret = m.invoke(obj, args);
        return ret;
    }

    void invokeStatic(String cls, String meth, Object[] args)
        throws Exception
    {
        Class<?> cl = loadClass(cls);
        Class<?>[] cl_args= new Class[args.length];
        for ( int i = 0; i < args.length; i++ ) {
            cl_args[i] = args[i].getClass();
        } 
        Method m = cl.getMethod(meth, cl_args);
        m.invoke(null, args);
    }


    class HttpClassLoader
        extends URLClassLoader
    {

        ClassLoader parent;
        HttpClassLoader(URL[] urls, ClassLoader parent)
        {
            super(urls, parent);
            this.parent = parent;

            //for ( URL u : urls ) {
            //    System.out.println("XStreamClassLoader initializes from " + u);
            // }
        }

        public Class<?> findClass(final String clname)
            throws ClassNotFoundException
        {
            if ( DEBUG ) System.out.println("--------- HttpClassLoader -- Finding class " + clname + " from " + this.getClass() + " " + clname);
            Class<?> ret = super.findClass(clname);
            if ( ret == null ) {
                if ( DEBUG ) {
                    System.out.println("                                          Not found");
                }
            } else {
                if ( DEBUG ) {
                    System.out.println("                                          " + ret.getProtectionDomain().getCodeSource().getLocation());
                    System.out.println("                                          " + ret.getClassLoader());
                }
            }
            return ret;
        }

    }

    class PrivateClassLoader
        extends ClassLoader
    {
        ClassLoader parent;
        HttpClassLoader grand_parent;
        /**
         * This creates a private class loader that loads only the things
         * the we publicly expose to the user of the CLI/API.
         *
         * The parent loads should be the primal class loader, the parent
         * of getSystemClassLoader(), which gives access to the base Java
         * classes.
         *
         * There are some interfaces that exist in both the 'public' and
         * the 'private' cli.  These guys must be loaded by the same
         * loader regardless of whether public or private classes
         * implement them, in order to get assignment compatibility.
         *
         * So this loader checks, if one of the 'shared' classes is being
         * loaded, and if so, it loads them off the user's classpath,
         * i.e. the also_parent loader, i.e. the public classpath, instead
         * of from the private classpath, to insure assignemnt
         * compatibility.
         *
         */
        PrivateClassLoader(ClassLoader parent, HttpClassLoader grand_parent)
        {
            super(parent);
            this.parent = parent;
            this.grand_parent = grand_parent;
        }


        public Class<?> loadClass(String clname)
        	throws ClassNotFoundException
        {
            if ( DEBUG ) System.out.println("---- A ------- load class " + clname + " from " + this);
            Class<?> ret =  super.loadClass(clname);
            if ( DEBUG ) {
                System.out.println("---- A ------- load class " + clname + " returns " + ret );
                try { 
                    throw new Exception("Stack trace:");
                } catch ( Throwable e ) {
                    e.printStackTrace();
                    System.out.println("--- A -----------------------------------------------------------------------------");
                }
            }
            return ret;
        }

        public Class<?> loadClass(String clname, boolean resolve)
        	throws ClassNotFoundException
        {

            if ( DEBUG ) System.out.println("---- B ------- load class " + clname + " from " + this);
            Class<?> ret =  super.loadClass(clname, resolve);
            if ( DEBUG ) {
                System.out.println("---- B ------ load class " + clname + " returns " + ret);
                try { 
                    throw new Exception("Stack trace:");
                } catch ( Throwable e ) {
                    e.printStackTrace();
                    System.out.println("---- B -----------------------------------------------------------------------------");
                }
            }
            return ret;
        }


        public Class<?> findClass(final String clname)
            throws ClassNotFoundException
        {
            if ( DEBUG ) System.out.println("*********** find class ********** " + clname);
            // we want all the interfaces from "common" except UIOptions and IDuccMonitor which aren't well-placed
            // for this exercise so we need a special case for them
            Class<?> ret = grand_parent.findClass(clname);
            if ( ret == null ) {
                if ( DEBUG ) {
                    System.out.println("*********** Looking in " + parent + " for " + clname);
                }
                return super.findClass(clname);
            } else {
                return ret;
            }
        }

    }

}
