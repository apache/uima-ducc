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
package org.apache.uima.ducc.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.uima.ducc.common.utils.DuccLogger;
import org.apache.uima.ducc.common.utils.DuccProperties;
import org.apache.uima.ducc.common.utils.IllegalConfigurationException;




/**
 * This class reads and parses a node configuration file.  It is used primarily by RM for scheduling
 * and by the web server to present the configuration.
 */
public class NodeConfiguration 
{
    String config_file_name = null;
    BufferedReader in;
    int lineno = 0;
    DuccProperties defaultFairShareClass  = new DuccProperties();
    DuccProperties defaultFixedShareClass = new DuccProperties();
    DuccProperties defaultReserveClass    = new DuccProperties();
    DuccProperties defaultNodepool        = new DuccProperties();

    Map<String, DuccProperties> np_set;
    Map<String, DuccProperties> class_set;

    Map<String, String> allNodefiles;

    DuccLogger logger;
    String defaultDomain = null;
    String firstNodepool = null;

    DuccProperties fairShareDefault = null;
    DuccProperties reserveDefault   = null;
    String ducc_home = null;
    String default_domain = null;

    public NodeConfiguration(String config_file_name, DuccLogger logger)
    {
        this.config_file_name = config_file_name;
        this.logger = logger;
        this.allNodefiles = new HashMap<String, String>();
        
        ducc_home = System.getProperty("DUCC_HOME");
        
        defaultFairShareClass.put("type", "class");
        defaultFairShareClass.put("name", "defaultFairShareClass");
        defaultFairShareClass.put("policy", "FAIR_SHARE");
        defaultFairShareClass.put("weight", "100");
        defaultFairShareClass.put("priority", "10");
        defaultFairShareClass.put("cap", Integer.toString(Integer.MAX_VALUE));
        defaultFairShareClass.put("expand-by-doubling", "true");
        defaultFairShareClass.put("initialization-cap", "2");
        defaultFairShareClass.put("use-prediction", "true");
        defaultFairShareClass.put("max-processes", Integer.toString(Integer.MAX_VALUE));
        defaultFairShareClass.put("prediction-fudge", "60000");
        defaultFairShareClass.put("nodepool", "<required>");
        defaultFairShareClass.put("debug", "fixed");
        defaultFairShareClass.put("abstract", "<optional>");
        defaultFairShareClass.put("children", "<optional>");
        defaultFairShareClass.put("parent", "<optional>");
        defaultFairShareClass.put("debug", "<optional>");
        defaultFairShareClass.put("default", "<optional>");
        defaultFairShareClass.put("name", "<required>");    // required, but always filled in by the parser.  needed here for validation.

        defaultFixedShareClass.put("type", "class");
        defaultFixedShareClass.put("name", "defaultFixedShareClass");
        defaultFixedShareClass.put("abstract", "<optional>");
        defaultFixedShareClass.put("children", "<optional>");
        defaultFixedShareClass.put("parent", "<optional>");
        defaultFixedShareClass.put("policy", "FIXED_SHARE");
        defaultFixedShareClass.put("priority", "5");
        defaultFixedShareClass.put("default", "<optional>");
        defaultFixedShareClass.put("max-processes", Integer.toString(Integer.MAX_VALUE));
        defaultFixedShareClass.put("cap", Integer.toString(Integer.MAX_VALUE));
        defaultFixedShareClass.put("nodepool", "<required>");

        defaultReserveClass.put("type", "class");
        defaultReserveClass.put("name", "defaultReserveClass");
        defaultReserveClass.put("abstract", "<optional>");
        defaultReserveClass.put("children", "<optional>");
        defaultReserveClass.put("parent", "<optional>");
        defaultReserveClass.put("policy", "RESERVE");
        defaultReserveClass.put("priority", "1");
        defaultReserveClass.put("default", "<optional>");
        defaultReserveClass.put("max-machines", Integer.toString(Integer.MAX_VALUE));
        defaultReserveClass.put("cap", Integer.toString(Integer.MAX_VALUE));
        defaultReserveClass.put("nodepool", "<required>");
        defaultReserveClass.put("enforce", "true");

        defaultNodepool.put("type", "nodepool");
        defaultNodepool.put("name", "<optional>");
        defaultNodepool.put("nodefile", "<optional>");
        defaultNodepool.put("parent", "<optional>");
        defaultNodepool.put("domain", "<optional>");
     }

    /**
     * Resolve a filename relative to DUCC_HOME
     */
    String resolve(String file)
        throws IllegalConfigurationException
    {
        if ( !file.startsWith("/") ) {
            file = ducc_home + "/resources/" + file;
        }
        File f = new File(file);
        if ( ! f.exists() ) {
            throw new IllegalConfigurationException("File " + file + " does not exist or cannot be read.");
        }
        return file;
    }

    void logInfo(String methodName, String message)
    {
        if ( logger == null ) {
            System.out.println(message);
        } else {
            logger.info(methodName, null, message);
        }    
    }

    void logWarn(String methodName, String message)
    {
        if ( logger == null ) {
            System.out.println(message);
        } else {
            logger.warn(methodName, null, message);
        }
    }

    void logError(String methodName, String message)
    {
        if ( logger == null ) {
            System.out.println(message);
        } else {
            logger.error(methodName, null, message);
        }
    }
    
    /**
     * Use the NodeIdentity to infer my the domain name.
     *
     * Itertate through the possible names - if one of them has a '.'
     * the we have to assume the following stuff is the domain name.
     * We only get one such name, so we give up the search if we find
     * it.
     */
    private String getDomainName()
    {
    	// String methodName = "getDomainName";

        if ( defaultDomain != null ) return defaultDomain;

        InetAddress me = null;
		try {
			me = InetAddress.getLocalHost();
		} catch (UnknownHostException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
        String my_happy_name = me.getHostName();
        String my_canonical_name = me.getCanonicalHostName();
        
        if ( my_canonical_name.startsWith(my_happy_name) ) {
            int ndx = my_canonical_name.indexOf(".");
            return my_canonical_name.substring(ndx+1);
        }
        return null;
    }

    Map<String, String> allNodes = new HashMap<String, String>();               // To insure no duplicates
    Map<String, String> readNodepoolFile(String npfile, String domain, boolean skip)
    	throws IllegalConfigurationException
    {
    	//String methodName = "readNodepoolFile";
        allNodefiles.put(npfile, npfile);
        String ducc_home = System.getProperty("DUCC_HOME");
        npfile = resolve(npfile);

        Map<String, String> response = new HashMap<String, String>();
        
        try {
            BufferedReader br = new BufferedReader(new FileReader(npfile));
            String node = "";
            while ( (node = br.readLine()) != null ) {
                int ndx = node.indexOf("#");
                if ( ndx >= 0 ) {
                    node = node.substring(0, ndx);
                }
                node = node.trim();
                if (node.equals("") ) {
                    continue;
                }

                if ( node.startsWith("domain") ) {
                    String[] tmp = node.split("\\s");
                    if ( tmp.length == 2 ) {
                        domain = tmp[1];
                        continue;
                    } else {
                        throw new IllegalConfigurationException("Invalid domain specification in node file " + npfile + ": " + node);
                    }
                }

                if ( node.startsWith("import") ) {
                    String[] tmp = node.split("\\s");
                    if ( allNodefiles.containsKey(tmp[1]) ) {
                        if ( skip ) continue;
                        throw new IllegalConfigurationException("Duplicate imported nodefile " + tmp[1] + " found in " + npfile + ", not allowed.");
                    }
                    response.putAll(readNodepoolFile(tmp[1], domain, skip));
                    continue;
                }

                if ( allNodes.containsKey(node) ) {
                    throw new IllegalConfigurationException("Duplicate node found in " + npfile + ": " + node + "; first occurance in " + allNodes.get(node));
                }
                allNodes.put(node, npfile);
                response.put(node, node);

                // include fully and non-fully qualified names to allow sloppiness of config

                ndx = node.indexOf(".");
                String dnode = null;
                if ( ndx >= 0 ) {
                    dnode = node.substring(0, ndx);
                    response.put(dnode, dnode);
                } else if ( domain != null ) {
                    dnode = node + "." + domain;
                    response.put(dnode, dnode);
                } 
                if( dnode != null ) {
                    if ( allNodes.containsKey(dnode) ) {
                        throw new IllegalConfigurationException("Duplicate node found in " + npfile + ": " + dnode + "; first occurance in " + allNodes.get(dnode));
                    }
                    allNodes.put(dnode, npfile);
                }

            }
            br.close();                        
            
        } catch (FileNotFoundException e) {
            throw new IllegalConfigurationException("Cannot open NodePool file \"" + npfile + "\": file not found.");
        } catch (IOException e) {
            throw new IllegalConfigurationException("Cannot read NodePool file \"" + npfile + "\": I/O Error.");
        }
        
//         for (String s : response.keySet() ) {
//             System.out.println(npfile + ": " + s);
//         }
        return response;
    }

    /**
     * Provide a continual stream of lines, removing empty lines and comment lines.
     */
    String readLine()
    	throws IOException
    {
        String line = null;
        while ( (line = in.readLine()) != null ) {
            lineno++;
            // System.out.println("Line[" + lineno + "]: " + line);
            line = line.trim();
            if ( line.equals("") )      continue;
            if ( line.startsWith("#") ) continue;
            return line;
        }
        return null;
    }

    /**
     * Fill up the token buffer.
     */
    StringTokenizer buf = null;
    boolean fillBuf()
    	throws IOException
    {
        while ( (buf == null) || !buf.hasMoreTokens() ) {
            String line = readLine();
            if ( line == null ) return false;
            buf = new StringTokenizer(line, "\n\t\r\f{} =;", true);
        }
        return true;
    }

    /**
     * Provide a continual stream of tokens, throwing out whitespace and semocolons
     */
    String nextToken()
    	throws IOException
    {
        while ( fillBuf() ) {
            String tok = null;
            while ( buf.hasMoreTokens() ) {
                tok = buf.nextToken();
                if ( tok.equals(" ") ) continue;
                if ( tok.equals("\t") ) continue;
                if ( tok.equals(";") ) continue;   // optional semicolon, ignored
                return tok;
            }
        }                         
        return null;
    }

    void parseInternal(DuccProperties props)
        throws IOException,
        IllegalConfigurationException
    {
//         System.out.println("Parsing nodepool " + name);
//         if ( parent == null ) {
//             System.out.println("     <base>");
//         } else {
//             System.out.println("     Inherits from " + parent);
//         }

        String tok = null;
        while ( (tok = nextToken() ) != null ) {
            if ( tok.equals("}") ) return;

            String k = tok;
            if ( k.equals("{") ) {
                throw new IllegalConfigurationException("Missing '}' near line " + lineno + " in " + config_file_name);
            }
            String v = nextToken();
            if ( v.equals("=") ) v = nextToken();     // (optionally allow k v  or k=v)

            if ( v.equals("}") ) {
                throw new IllegalConfigurationException("Missing value near line " + lineno + " in " + config_file_name);
            }
            if ( v.equals("{") ) {
                throw new IllegalConfigurationException("Missing '}' near line " + lineno + " in " + config_file_name);
            }

            // note we allow duplicate entries, which turn into a list
            if ( props.getProperty(k) == null ) {
                props.put(k, v);
            } else {
                throw new IllegalConfigurationException("Duplicate property near line " + lineno + " in " + config_file_name + ": " + k);
            }
        }
        return;
    }

    DuccProperties parseNodepool(String name, String parent)
        throws IOException,
        IllegalConfigurationException
    {
//         System.out.println("Parsing nodepool " + name);
//         if ( parent == null ) {
//             System.out.println("     <base>");
//         } else {
//             System.out.println("     Inherits from " + parent);
//         }

        if ( firstNodepool == null ) {
            firstNodepool = name;
        }

        DuccProperties ret = new DuccProperties();
        ret.put("type", "nodepool");
        ret.put("name", name);
        if ( parent != null ) {
            throw new IllegalConfigurationException("Illegal inheritance (inheritance not supported for nodepools) near line " + lineno + " in " + config_file_name);
        }

        parseInternal(ret);
        String dd = ret.getProperty("domain");
        if ( name.equals(firstNodepool) ) {
            defaultDomain = dd;
        } else {
            if ( dd != null ) {
                throw new IllegalConfigurationException("Default domain specified in nodepool other than first nodepool \"" + firstNodepool + "\", not allowed, near line " + lineno + " in " + config_file_name);
            }
        }            
        
        supplyDefaults(ret, defaultNodepool);

        return ret;
    }

    DuccProperties parseClass(String name, String parent)
    	throws IOException,
    	IllegalConfigurationException
    {
        DuccProperties ret = new DuccProperties();
        ret.put("type", "class");
        ret.put("name", name);
        if ( parent != null ) {
            ret.put("parent", parent);
        }

        parseInternal(ret);

        return ret;
    }

    ArrayList<DuccProperties> nodepools = new ArrayList<DuccProperties>();
    ArrayList<DuccProperties> classes = new ArrayList<DuccProperties>();
    DuccProperties parseStanzas()
    	throws IOException,
    	IllegalConfigurationException
    {
        String tok;
        while ( (tok = nextToken()) != null ) {
            String type = tok;                // stanza type

            String name = nextToken();        // stanza name
            if ( name == null ) {
                throw new IllegalConfigurationException("Missing stanza name near line " + lineno + " in " + config_file_name);
            }

            String parent = nextToken();      // who to inherit from, or "{"
            String start = null;
            if ( parent.equals("{") ) {
                start = parent;
                parent = null;
            } else {
                start = nextToken();
            }
            if ( ! start.equals("{") ) {
                throw new IllegalConfigurationException("Missing '{' near line " + lineno + " in " + config_file_name);
            }
            
            if ( type.equals("Nodepool") ) nodepools.add(parseNodepool(name, parent));
            if ( type.equals("Class") )    classes.add(parseClass(name, parent));
        }
        return null;
    }
    
    /**
     * Given the 'in' properties, look through the 'model' propertis and make sure there are
     * no unsupported properties, and that all required properties are filled in.  Works for both
     * Class and Nodepool properties.
     */
    void supplyDefaults(DuccProperties in, DuccProperties model)
    	throws IllegalConfigurationException
    {
        // first make sure all properties for the input object are valid
    	String name = in.getProperty("name");
        String type = in.getProperty("type");
        for (Object o : in.keySet()) {
            String k = (String) o;
            if ( model.get(k) == null ) {                // key not in model is illegal
                throw new IllegalConfigurationException("Illegal property \"" + k + "\" in " + type + " " + name);
            }
        }

        // now make sure all required fields are supplied and fill in defaults
        for ( Object o : model.keySet() ) {
            String k = (String) o;
            String vm = model.getProperty(k);
            String vi = in.getProperty(k);

            if ( vi == null)  {
                if ( vm.equals("<required>" ) ) {
                    throw new IllegalConfigurationException("Missing required property " + k + " in " + type + " " + name);
                }

                if ( vm.equals("<optional>") ) {     // its optional but there is no meaningful default
                    continue;
                }

                // System.out.println("Object " + name + " inherits " + k + "," + vm + " from " + model.get("name"));
                in.put(k, vm);                       // fill in the default
            }
        }
    }

    Map<String, DuccProperties> npmap = new HashMap<String, DuccProperties>();
    Map<String, DuccProperties> clmap = new HashMap<String, DuccProperties>();
    ArrayList<DuccProperties> independentNodepools = new ArrayList<DuccProperties>();
    ArrayList<String> independentClasses = new ArrayList<String>();

    /**
     * Propogate my properties to all my kids and their kids, etc.  Classes only, Nodepools don't
     * do any sort of inheritance.
     */
    void propogateDown(String clname)
    {
        DuccProperties cl = clmap.get(clname);
        String children = cl.getStringProperty("children", null);
        if ( children == null ) return;

        String[] kids = children.split("\\s+");
        for ( String kid : kids ) {
            DuccProperties kp = clmap.get(kid);

            for ( Object o : cl.keySet() ) {
                if ( ! kp.containsKey(o) ) {
                    String k = (String) o;
                    if ( k.equals("abstract" ) ) continue;       // don't propogate down abstractness
                    if ( k.equals("children" ) ) continue;       // don't propogate down children
                    if ( k.equals("default" ) ) continue;        // don't propogate down default
                    String v = cl.getStringProperty(k);
                    // System.out.println("Object " + kp.get("name") + " inherits " + k + "," + v + " from " + cl.get("name"));
                    kp.put(k, v);
                }                
            }
            propogateDown(kid);
        }
    }

    void handleDefault(DuccProperties p, DuccProperties def, String policy)
    	throws IllegalConfigurationException
    {
        String dflt = p.getProperty("default");
        if ( dflt != null ) {
            if ( def != null ) {
                throw new IllegalConfigurationException("Class " + p.getProperty("name") 
                                                        + ": Only one " + policy + " default allowed.  Already defined in class \"" 
                                                        + def.getProperty("name")
                                                        + "\"");
            } else {
                if ( policy.equals("FAIR_SHARE" ) ) {
                    fairShareDefault = p;
                } else {
                    reserveDefault = p;
                }
            }
        }
    }

    /**
     * Map all the classes and do inheritance.  
     * Check values for correctness.
     */
    void doClassInheritance()
    	throws IllegalConfigurationException
    {
        // map the clases, crash on duplicates
        for ( DuccProperties p : classes ) {
            String name = p.getStringProperty("name");
            if ( clmap.containsKey(name) ) {
                throw new IllegalConfigurationException("Duplicate class: " + name);
            }
            clmap.put(name, p);
        }
        
        // now establish the parent -> child relationships
        for ( DuccProperties p : clmap.values() ) {
            String parent = p.getProperty("parent");
            String name   = p.getProperty("name");
            
            if ( (p.getProperty("abstract") != null) &&
                 (p.getProperty("default") != null ) ) {
                throw new IllegalConfigurationException("Class " + name + ": Abstract class is not allowed to specify \"default\"");
            }

            
            if ( parent == null ) {
                independentClasses.add(name);
            } else {
                DuccProperties par_cl = clmap.get(parent);
                if ( par_cl == null ) {
                    throw new IllegalConfigurationException("Class " + name + " parent pool " + parent + " cannot be found.");
                }
                String children = par_cl.getStringProperty("children", null);
                if ( children == null ) {
                    children = name;
                } else {
                    children = children + " " + name;
                }
                par_cl.put("children", children);
            }
        }

        // now starting at every root, propogate stuff down
        for ( String s : independentClasses ) {
            propogateDown(s);
        }

        // must fill in defaults, which we couldn't do until we finished inheritance
        for ( DuccProperties p : clmap.values() ) {
            String policy = p.getStringProperty("policy", null);
            String name = p.getProperty("name");

            if ( policy == null ) {
                throw new IllegalConfigurationException("Class " + name + " is missing scheduling policy ");
            }
            if ( policy.equals("FAIR_SHARE") ) {
                handleDefault(p, fairShareDefault, policy);
                supplyDefaults(p, defaultFairShareClass);
            } else
            if ( policy.equals("FIXED_SHARE") ) {
                handleDefault(p, reserveDefault, policy);
                supplyDefaults(p, defaultFixedShareClass);
            } else
            if ( policy.equals("RESERVE") ) {
                handleDefault(p, reserveDefault, policy);
                supplyDefaults(p, defaultReserveClass);
            } else {
                throw new IllegalConfigurationException("Unknown scheduling policy \"" + policy + "\" in class " + name);
            }
        }

        // remove the abstract classes as they are no longer needed and we don't want to let them leak out
        // where somebody might think they're ok to use
        Iterator<String> iter = clmap.keySet().iterator();
        while ( iter.hasNext() ) {
            String k = iter.next();
            DuccProperties p = clmap.get(k);
            if ( p.containsKey("abstract") ) {
                // System.out.println("---------------- Remove " + p.get("name"));
                iter.remove();
            }
        }

    }

    /**
     * Find all the top-level nodepools.
     * Make sure every parent nodepool exists.
     * Set the names of the child nodepools into each parent.
     * Set the names of the classes managed in each nodepool into the nodepools.
     */
    void connectNodepools()
    	throws IllegalConfigurationException
    {
        // map the nodepools, crashing on duplicates
        for ( DuccProperties p : nodepools ) {
            String name = p.getStringProperty("name");
            if ( npmap.containsKey(name) ) {
                throw new IllegalConfigurationException("Duplicate nodepool: " + name);
            }

            npmap.put(name, p);
        }

        // map the child nodepools into their parents
        for ( DuccProperties p : nodepools ) {
            String parent = p.getStringProperty("parent", null);
            String name   = p.getStringProperty("name");
            if ( parent == null ) {
                independentNodepools.add(p);
            } else {
                DuccProperties par_pool = npmap.get(parent);
                if ( par_pool == null ) {
                    throw new IllegalConfigurationException("Nodepool " + name+ " parent pool " + parent + " cannot be found.");
                }
                @SuppressWarnings("unchecked")
				List<DuccProperties> children = (List<DuccProperties>) par_pool.get("children");
                if ( children == null ) {
                    children = new ArrayList<DuccProperties>();
                    par_pool.put("children", children);
                }
                children.add(p);
            }
        }

        // connect the classes into their nodepools
        for ( DuccProperties p : classes ) {
            if ( p.containsKey("abstract") ) continue;                // don't propogate these out

            String name = p.getStringProperty("name");

            String npname = p.getStringProperty("nodepool", null);
            if ( npname == null ) {
                throw new IllegalConfigurationException("Class " + name + " is not assigned to a nodepool.");
            }
            DuccProperties np = npmap.get(npname);
            if ( np == null ) {
                throw new IllegalConfigurationException("Class " + name + " assigned to nodepool " + npname + " but nodepool does not exist.");
            }

            @SuppressWarnings("unchecked")
			List<DuccProperties> class_set = (List<DuccProperties>) np.get("classes");
            if ( class_set == null ) {
                class_set = new ArrayList<DuccProperties>();
                np.put("classes", class_set);
            } 
            class_set.add(p);

        }        
    }

    void readNodefile(DuccProperties p, String domain)
        throws IllegalConfigurationException
    {
        String npfile = p.getProperty("nodefile");
        if ( npfile != null ) {
            p.put("nodes", readNodepoolFile(npfile, domain, false));
        }

        @SuppressWarnings("unchecked")
		List<DuccProperties> children = (List<DuccProperties>) p.get("children");
        if ( children != null ) {
            for ( DuccProperties pc : children ) {
                readNodefile(pc, domain);
            }
        }
    }

    /**
     * Read the complete node configuration as defined in.  Intended for use from command line, not
     * usually elsewhere.
     */
    void fullValidation(String global_nodefile)
        throws IllegalConfigurationException
    {
        global_nodefile = resolve(global_nodefile);
        if ( allNodefiles.containsKey(global_nodefile) ) return;           // already passed if is's there and we're here
        readNodepoolFile(global_nodefile, default_domain, true);                   // will throw if there's an issue
    }

    public DuccProperties getDefaultFairShareClass()
    {
        return fairShareDefault;
    }

    public DuccProperties getDefaultReserveClass()
    {
        return reserveDefault;
    }

    public DuccProperties[] getToplevelNodepools()
    {
        return independentNodepools.toArray(new DuccProperties[independentNodepools.size()]);
    }

    public DuccProperties getClass(String name)
    {
        if ( clmap == null ) return null;
        return clmap.get(name);
    }

    public Map<String, DuccProperties> getClasses()
    {
        return clmap;
    }

    public void readConfiguration()
        throws FileNotFoundException, 
        IOException,
        IllegalConfigurationException
    {
        if ( ducc_home == null ) {
            throw new IllegalConfigurationException("DUCC_HOME must be defined as a system property.");
        }

        config_file_name = resolve(config_file_name);
        in = new BufferedReader(new FileReader(config_file_name));
        default_domain = getDomainName();

        parseStanzas();
        doClassInheritance();
        connectNodepools();
       
        for (DuccProperties p : independentNodepools) {      // walk the tree and read the node files
            readNodefile(p, default_domain);
        }

        try {
			in.close();
		} catch (IOException e) {
            // nothing ... who cares if we got this far, and what can we do anyway ?
		}
    }

    void printNodepool(DuccProperties p, String indent)
    {
        String methodName = "printNodepool";

        logInfo(methodName, indent + "Nodepool " + p.getProperty("name"));
        String nodefile = p.getProperty("nodeile");
        logInfo(methodName, indent + "   Node File: " + (nodefile == null ? "None" : nodefile));
        
        @SuppressWarnings("unchecked")
		List<DuccProperties> class_set = (List<DuccProperties>) p.get("classes");
        if ( class_set == null ) {
            logInfo(methodName, indent + "   No classes defined.");
        } else {
            StringBuffer buf = new StringBuffer();
            buf.append(indent + "   Classes:");
            for (DuccProperties cp : class_set ) {
                buf.append(" " + cp.get("name"));
            }
            logInfo(methodName, buf.toString());
        }

        @SuppressWarnings("unchecked")
		List<DuccProperties> children = (List<DuccProperties>) p.get("children");
        
        if ( children == null ) {
            logInfo(methodName, indent + "   No subpools.\n");
        } else {
            StringBuffer buf = new StringBuffer();
            buf.append(indent + "   Subpools:");            
            for ( DuccProperties cp : children ) {
                buf.append(" " + cp.get("name"));
            }
            logInfo(methodName, buf.toString());
            for ( DuccProperties cp : children ) {
                printNodepool(cp, indent + indent);
            }
        }
    }

    void printProperty(String k, Object v)
    {
    	String methodName = "printProperty";
        if ( v == null ) return;  // ignore non-existant properties
        logInfo(methodName, String.format("   %-20s: %s", k, v));
    }

    /**
     * Print class values in controlled order and format, not to be confused
     * with what you'd get just iterating the map.
     */
    void printClass(DuccProperties cl)
    {        
    	String methodName = "printClass";
        logInfo(methodName, "Class " +       cl.get("name"));
        printProperty("Policy",             cl.get("policy"));
        printProperty("Nodepool",           cl.get("nodepool"));
        printProperty("Priority",           cl.get("priority"));
        printProperty("Weight",             cl.get("weight"));
        printProperty("Debug",              cl.get("debug"));
        printProperty("Cap",                cl.get("cap"));
        printProperty("Expand By Doubling", cl.get("expand-by-doubling"));
        printProperty("Initialization Cap", cl.get("initialization-cap"));
        printProperty("Use Prediction",     cl.get("use-prediction"));
        printProperty("Prediction Fudge",   cl.get("prediction-fudge"));
        printProperty("Max Processes",      cl.get("max-processes"));
        printProperty("Max Machines",       cl.get("max-machines"));
        printProperty("Enforce Memory Size",cl.get("enforce"));

        logInfo(methodName, "");
    }

    public void printConfiguration()
    {
        String methodName = "printConfiguration";
        
        if ( independentNodepools == null || clmap == null ) {
            logError(methodName, "Configuration has not been generated. (Run readConfiguration first.)");
        }

        // First iterate nodepools
        for ( DuccProperties p : independentNodepools ) {          // it's a tree, use recursion
            printNodepool(p, "   ");
        }

        DuccProperties[] class_set = clmap.values().toArray(new DuccProperties[clmap.size()]);
        Arrays.sort(class_set, new ClassSorter());

        for ( DuccProperties p : class_set ) {
            printClass(p);
        }
    }

    static void usage(String msg)
    {
        if ( msg != null ) {
            System.out.println(msg);
            System.out.println("");
        }
        System.out.println("Usage:");
        System.out.println("    NodeConfiguration [-p] [-v nodefile] <configfile>");
        System.out.println("Where:");
        System.out.println("    -p does validation and prints the configuration with all defaults filled in");
        System.out.println("       If omitted, validation only is performed.");
        System.out.println("    -v <nodefile> does full node validation against the startup nodefile.  This is useful");
        System.out.println("       when there are configured nodes not explicitly defined to a nodepool.");
        System.out.println("    <configfile is the node configuration in ducc_runtime/resources.");
        System.out.println("     -? show this help.");
        System.out.println("");
        System.exit(1);
    }

    /**
     * Testing and verification of the file
     */
    public static void main(String[] args) 
    {

        boolean doprint = false;
        boolean validate_nodes = false;
        String global_nodefile = null;
        String config = null;

        int i = 0;

        // (simplistic, expects to be called from a script that does rigorous argument checking
        //              and setup)
        for ( i = 0; i < args.length; i++ ) {
            if ( args[i].equals("-p") ) {
                doprint = true;
                continue;
            }

            if ( args[i].equals("-v") ) {
                validate_nodes = true;
                global_nodefile = args[i+1];
                i++;
                continue;
            }

            if ( args[i].equals("-?") ) {
                usage(null);
            }
            
            config = args[i];
        }
            
        NodeConfiguration nc = new NodeConfiguration(config, null);

        int rc = 0;
        try {
            nc.readConfiguration();                        // if it doesn't crash it must have worked

            if ( validate_nodes ) {
                nc.fullValidation(global_nodefile);        // this too, gonna throw if there's an issue
            }

            if ( doprint ) {
                nc.printConfiguration();
            }
		} catch (FileNotFoundException e) {
            System.out.println("Configuration file " + config + " does not exist or cannot be read.");
            rc = 1;
		} catch (IOException e) {
            System.out.println("IOError reading configuration file " + config + ": " + e.toString());
            rc = 1;
        } catch (IllegalConfigurationException e) {
        	System.out.println(e.getMessage());
            rc = 1;
        }

        System.exit(rc);
    }

    static public class ClassSorter
        implements Comparator<DuccProperties>
    {
        public int compare(DuccProperties pr1, DuccProperties pr2)
        {
            // primary sort thus: FAIR_SHARE, FIXED_SHARE, RESERVE
            // seconddary sort on class name

        	
            if ( pr1.equals(pr2) ) return 0;         // deal with the 'equals' contract

            String p1 = pr1.getProperty("policy");
            String p2 = pr2.getProperty("policy");
            
            // happily, the string values sort the way we want so we can eliminate a lot of stuff
            // by just checking equality.
            if ( ! p1.equals(p2) ) {
                return p1.compareTo(p2);
            }

            // and if they're the same, just tiebreak on the name
            String n1 = pr1.getProperty("name");
            String n2 = pr2.getProperty("name");
            return n1.compareTo(n2);
        }
    }

}
