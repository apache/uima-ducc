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
import org.apache.uima.ducc.common.utils.SystemPropertyResolver;

/**
 * This class reads and parses a node configuration file.  It is used primarily by RM for scheduling
 * and by the web server to present the configuration.
 */
public class NodeConfiguration 
{
    String config_file_name = null;
    String ducc_nodes = null;
    String ducc_users = null;
    BufferedReader in;
    int lineno = 0;
    DuccProperties defaultFairShareClass  = new DuccProperties();
    DuccProperties defaultFixedShareClass = new DuccProperties();
    DuccProperties defaultReserveClass    = new DuccProperties();
    DuccProperties defaultNodepool        = new DuccProperties();
    DuccProperties defaultUser            = new DuccProperties();                         // UIMA-4275

    Map<String, DuccProperties> nodepools = new HashMap<String, DuccProperties>();        // all nodepools, by name
    ArrayList<DuccProperties> independentNodepools = new ArrayList<DuccProperties>();     // the top-level node pools

    // UIMA-4142 Move all these declarations to the top and comment them
    List<DuccProperties> classes = new ArrayList<DuccProperties>();                       // all classes, during parsing, a discard
    Map<String, DuccProperties> clmap    = new HashMap<String, DuccProperties>();         // all classes, by name
    Map<String, DuccProperties> usermap  = new HashMap<String, DuccProperties>();         // all users, by name UIMA-4275
    ArrayList<String> independentClasses = new ArrayList<String>();                       // all classes that don't derive from something

    Map<String, String> allNodes   = new HashMap<String, String>();                        // map node           -> nodepool name, map for dup checking
    Map<String, DuccProperties> poolsByNodefile = new HashMap<String, DuccProperties>();   // nodepool node file -> nodepool props

    Map<String, String> allImports = new HashMap<String, String>();                        // map nodefile -> importer, map for dup checking
    Map<String, String> referrers  = new HashMap<String, String>();                        // map nodefile -> referring nodepool, for dup checking

    DuccLogger logger;
    String defaultDomain = null;
    String firstNodepool = null;

    boolean fairShareExists = false;
    boolean fixedExists = false;
    boolean reserveExists = false;
    DuccProperties fairShareDefault = null;
    DuccProperties fixedDefault     = null;
    DuccProperties reserveDefault   = null;
    String ducc_home = null;

    public NodeConfiguration(String config_file_name, String ducc_nodes, String ducc_users, DuccLogger logger)
    {
        this.config_file_name = config_file_name;
        this.ducc_nodes = ducc_nodes;
        this.ducc_users = ducc_users;
        this.logger = logger;
        
        ducc_home = System.getProperty("DUCC_HOME");
        
        defaultFairShareClass.put("type", "class");
        defaultFairShareClass.put("name", "defaultFairShareClass");
        defaultFairShareClass.put("policy", "FAIR_SHARE");
        defaultFairShareClass.put("weight", "100");
        defaultFairShareClass.put("priority", "10");
        defaultFairShareClass.put("cap", Integer.toString(Integer.MAX_VALUE));
        defaultFairShareClass.put("expand-by-doubling", ""+SystemPropertyResolver.getBooleanProperty("ducc.rm.expand.by.doubling", true));
        defaultFairShareClass.put("initialization-cap", ""+SystemPropertyResolver.getIntProperty("ducc.rm.initialization.cap", 2));
        defaultFairShareClass.put("use-prediction", ""+SystemPropertyResolver.getBooleanProperty("ducc.rm.prediction", true));
        defaultFairShareClass.put("prediction-fudge", ""+SystemPropertyResolver.getIntProperty("ducc.rm.prediction.fudge", 60000));
        defaultFairShareClass.put("max-processes", "<optional>");   // this is deprecated. it remains here to ease migration to DUCC 2.0.  It has no effect.
        defaultFairShareClass.put("nodepool", "<required>");
        defaultFairShareClass.put("users", "<optional>");
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
        defaultFixedShareClass.put("max-processes", "<optional>");   // this is deprecated. it remains here to ease migration to DUCC 2.0.  It has no effect.
        defaultFixedShareClass.put("cap", "<optional>");             // this is deprecated. it remains here to ease migration to DUCC 2.0.  It has no effect.
        defaultFixedShareClass.put("nodepool", "<required>");
        defaultFixedShareClass.put("users", "<optional>");

        defaultReserveClass.put("type", "class");
        defaultReserveClass.put("name", "defaultReserveClass");
        defaultReserveClass.put("abstract", "<optional>");
        defaultReserveClass.put("children", "<optional>");
        defaultReserveClass.put("parent", "<optional>");
        defaultReserveClass.put("policy", "RESERVE");
        defaultReserveClass.put("priority", "1");
        defaultReserveClass.put("default", "<optional>");
        defaultReserveClass.put("max-machines", "<optional>");       // this is deprecated. it remains here to ease migration to DUCC 2.0.  It has no effect.
        defaultReserveClass.put("cap", "<optional>");                // this is deprecated. it remains here to ease migration to DUCC 2.0.  It has no effect.
        defaultReserveClass.put("nodepool", "<required>");
        defaultReserveClass.put("users", "<optional>");
        defaultReserveClass.put("enforce", "true");

        defaultNodepool.put("type", "nodepool");
        defaultNodepool.put("name", "<optional>");
        defaultNodepool.put("nodefile", "<optional>");
        defaultNodepool.put("parent", "<optional>");
        defaultNodepool.put("share-quantum", "<optional>");
        defaultNodepool.put("domain", "<optional>");
        defaultNodepool.put("search-order", "100");    // temporary.  UIMA-4324

        defaultUser.put("type", "user");
        defaultUser.put("name", "<optional>");
        defaultUser.put("max-allotment", Integer.toString(Integer.MAX_VALUE));
     }

    /**
     * Resolve a filename relative to DUCC_HOME
     */
    String resolve(String file)
        throws IllegalConfigurationException
    {
    		if ( file == null ) return null;
        if ( !file.startsWith("/") ) {
            file = ducc_home + "/resources/" + file;
        }
        File f = new File(file);
        if ( ! f.exists() ) {
            return null;              // UIMA-4275 Defer crash to caller, making it optional.
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
            return line + ";";          // convert linend into ; so we can do lists
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
            buf = new StringTokenizer(line, "\n\t\r\f{} =,;", true);
        }
        return true;
    }

    /**
     * Provide a continual stream of tokens, throwing out whitespace and semocolons
     */
    String pushback = null;
    String nextToken()
        throws IOException
    {
        if ( pushback != null ) {
            // System.out.println("Return " + pushback + " from pushback.");
            String ret = pushback;
            pushback = null;
            return ret;
        }

        while ( fillBuf() ) {
            String tok = null;
            while ( buf.hasMoreTokens() ) {
                tok = buf.nextToken();
                // System.out.println("Token: " + tok);
                if ( tok.equals(" ") ) continue;   // ignoring whitespace
                if ( tok.equals("\t") ) continue;  // ignoring whitespace
                if ( tok.equals(",") )  continue;  // ignoring commas as whitespace

                return tok;
            }
        }                         
        return null;
    }

    /**
     * Consume the token stream up to the next delimeter.
     */
    String consume()
    	    throws IOException
    {
        String tok = nextToken();
        if ( tok.equals("=") ) tok = nextToken();          // optional '='
        if ( tok.equals("}") ) return tok;                 // start of stream is }, probably invalid
        if ( tok.equals("{") ) return tok;                 // start of stream is {, probably invalid

        String ret = null;
        while ( tok != null ) {
            if ( tok.equals(";") ) return ret;            // logical eol

            if ( tok.equals("}") || tok.equals("{") ) {   // start or begin of stanza, not at start of stream
                pushback = tok;                           // we allow 1 token pushback
                return ret;
            }
            if ( ret == null ) {
                ret = tok;
            } else {
                ret = ret + " " + tok;
            }
            tok = nextToken();
        }
        return ret;
    }

    void parseInternal(DuccProperties props)
        throws IOException,
        IllegalConfigurationException
    {
        String tok = null;
        while ( (tok = nextToken() ) != null ) {
        		if ( tok.equals(";") ) continue;
            if ( tok.equals("}") ) return;

            String k = tok;
            if ( k.equals("{") ) {
                throw new IllegalConfigurationException("Missing '}' near line " + lineno + " in " + config_file_name);
            }

            String v = consume();

            if ( v.equals("}") ) {
                throw new IllegalConfigurationException("Missing value near line " + lineno + " in " + config_file_name);
            }
            if ( v.equals("{") ) {
                throw new IllegalConfigurationException("Missing '}' near line " + lineno + " in " + config_file_name);
            }

            if ( ! props.containsKey(k) ) {
                props.put(k, v);
            } else {
                throw new IllegalConfigurationException("Duplicate property not allowed near line " + lineno + " in " + config_file_name + ": " + k);
            }
        }
        return;
    }

    DuccProperties parseNodepool(String name, String parent)
        throws IOException,
        IllegalConfigurationException
    {

        if ( firstNodepool == null ) {
            firstNodepool = name;
        }

        DuccProperties ret = new DuccProperties();
        ret.put("type", "nodepool");
        ret.put("name", name);
        if ( parent != null ) {
            throw new IllegalConfigurationException("Illegal inheritance (inheritance not supported for nodepools) near line " + lineno + " in " + config_file_name);
        }

        // put into global map
        if ( nodepools.containsKey(name) ) {
            throw new IllegalConfigurationException("Duplicate nodepool: " + name);
        }
        nodepools.put(name, ret);

        parseInternal(ret);
        String dd = ret.getProperty("domain");
        if ( name.equals(firstNodepool) && (dd != null) ) {
            defaultDomain = dd;
        } else {
            if ( dd != null ) {
                throw new IllegalConfigurationException("Default domain specified in nodepool other than first nodepool \"" + firstNodepool + "\", not allowed, near line " + lineno + " in " + config_file_name);
            }
        }            
        
        supplyDefaults(ret, defaultNodepool);
        if ( ! ret.containsKey("nodefile") && (ducc_nodes != null) ) {
        	// duplicates will be checked later
            ret.put("nodefile", ducc_nodes);
        }
        if ( ! ret.containsKey("parent") ) {
            // System.out.println("Add top level nodepool: " + name);
            independentNodepools.add(ret);
        }
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

    // UIMA-4275
    DuccProperties parseUser(String name, String parent)
        throws IOException,
        IllegalConfigurationException
    {
        DuccProperties ret = new DuccProperties();
        ret.put("type", "user");
        ret.put("name", name);
        if ( parent != null ) {
            ret.put("parent", parent);
        }

        parseInternal(ret);

        return ret;
    }

    DuccProperties parseStanzas()
        throws IOException,
        IllegalConfigurationException
    {
        String tok;
        while ( (tok = nextToken()) != null ) {

            if ( tok.equals(";") ) continue;  // logical EOL, ignore here
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
            
            if ( type.equals("Nodepool") ) nodepools.put(name, parseNodepool(name, parent));
            if ( type.equals("Class") )    classes.add(parseClass(name, parent));
            if ( type.equals("User") )     usermap.put(name, parseUser(name, parent));       // UIMA-4275
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

                if ( vm.contains("<optional>") ) {     // its optional but there is no meaningful default
                    continue;
                }

                // System.out.println("Object " + name + " inherits " + k + "," + vm + " from " + model.get("name"));
                in.put(k, vm);                       // fill in the default
            }
        }
    }

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
                } else if ( policy.equals("FIXED_SHARE") ) {
                    fixedDefault = p;
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
                fairShareExists = true;
                handleDefault(p, fairShareDefault, policy);
                supplyDefaults(p, defaultFairShareClass);
            } else
            if ( policy.equals("FIXED_SHARE") ) {
                fixedExists = true;
                handleDefault(p, fixedDefault, policy);
                supplyDefaults(p, defaultFixedShareClass);
            } else
            if ( policy.equals("RESERVE") ) {
                reserveExists = true;
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
     * Look at all top-level nodepools and insure the scheduling quantum is set.  If not, inherit it from
     * ducc.properties.
     */
    void setShareQuantum()
    	throws IllegalConfigurationException
    {
        for (DuccProperties props : independentNodepools ) {
            String q = props.getProperty("share-quantum");
            if ( q == null ) {
                props.setProperty("share-quantum", ""+SystemPropertyResolver.getIntProperty("ducc.rm.share.quantum", 15));
            } else {
                try {
					Integer.parseInt(q);      // insure it's a number
                } catch (NumberFormatException e) {
                    throw new IllegalConfigurationException("Value for \"share-quantum\" in nodepool " + props.getProperty("name") + " is not a number.");
                }
            }
        }
    }

    /**
     * (Recursively) walk up parental chain to find the top-level np for this guy.
     */
    DuccProperties getTopLevel(DuccProperties child)
    {
        String parent = child.getStringProperty("parent", null);
        if ( parent == null ) return child;
        return getTopLevel(nodepools.get(parent));
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

        // Insure default is set right
        setShareQuantum();

        // Map the child nodepools into their parents
        for ( DuccProperties p : nodepools.values() ) {
            String parent = p.getStringProperty("parent", null);
            String name   = p.getStringProperty("name");

            if ( parent != null ) {

                // Insure no scheduling quantum is set
                if ( p.getProperty("share-quantum") != null ) {
                    throw new IllegalConfigurationException("Nodepool " + name + ": \"share-quantum\" is legal only for top-level nodepools.");
                }                

                // Now pull down the parent's scheduling quantum
                DuccProperties tl = getTopLevel(p);
                p.setProperty("share-quantum", tl.getProperty("share-quantum"));  // (guaranteed non-null by setSchedulingQuantum)

                // And now connect children and parents
                DuccProperties par_pool = nodepools.get(parent);
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
            DuccProperties np = nodepools.get(npname);
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

    void readNodepoolNodes(String nodefile, DuccProperties p, String domain)
    		throws IllegalConfigurationException
    {    	
    	String methodName = "readnodepoolFiles";
        @SuppressWarnings("unchecked")
        Map<String, String> nodes = (Map<String, String>) p.get("nodes");
        if ( nodes == null ) {
            nodes = new HashMap<String, String>();
            p.put("nodes", nodes);
        }

        BufferedReader br = null;
        try {
            String tnodefile = resolve(nodefile);
            if ( tnodefile == null ) {
                throw new IllegalConfigurationException("File " + nodefile + " does not exist.");
            }
            nodefile = tnodefile;

            br = new BufferedReader(new FileReader(nodefile));
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

                // UIMA-4142 Domain no longer supported
                if ( node.startsWith("domain") ) {
                    logInfo(methodName, "The \"domain\" keyword is no long supported in node files. Found in file " + nodefile);
                    continue;
                }


                if ( node.startsWith("import") ) {
                    // if it's an import and it's not in the set of nodepool files, read it in3
                    // otherwise ignore it because it will get read and associated with it's correct
                    // nodepool
                    String[] tmp = node.split("\\s+");
                    String importfile = tmp[1];
                    if ( ! poolsByNodefile.containsKey(importfile) ) {
                        readNodepoolNodes(importfile, p, domain);
                    }
                    continue;
                }
                
                if ( allNodes.containsKey(node) ) {
                    throw new IllegalConfigurationException("Duplicate node found in " + nodefile + ": " + node + "; first occurance in " + allNodes.get(node));
                }
                allNodes.put(node, nodefile); // for dup checking - we only get to read a node once
                nodes.put(node, nodefile);    // UIMA-4142 map host -> domain

                // include fully and non-fully qualified names to allow sloppiness of config
                ndx = node.indexOf(".");
                String dnode = null;
                if ( ndx >= 0 ) {                     // strip domain to get just the name
                    dnode = node.substring(0, ndx);
                    nodes.put(dnode, nodefile);      // UIMA-4142 map host -> domain
                } else if ( domain != null ) {       // or add the domain, if found, to get fully-qualified
                    dnode = node + "." + domain;
                    nodes.put(dnode, nodefile);      // UIMA-4142 map host -> domain
                } 
                if( dnode != null ) {
                    if ( allNodes.containsKey(dnode) ) {
                        throw new IllegalConfigurationException("Duplicate node found in " + nodefile + ": " + dnode + "; first occurance in " + allNodes.get(dnode));
                    }
                    allNodes.put(dnode, nodefile);   // UIMA-4142 map host -> domain
                }
            }
            
        } catch (FileNotFoundException e) {
            throw new IllegalConfigurationException("Cannot open NodePool file \"" + nodefile + "\": file not found.");
        } catch (IOException e) {
            throw new IllegalConfigurationException("Cannot read NodePool file \"" + nodefile + "\": I/O Error.");
        } catch (IllegalConfigurationException e) {
            throw e;
        } catch ( Exception e ) {
        		e.printStackTrace();
        } finally {
            if ( br != null ) {
                try { br.close(); } catch (IOException e) { }
            }
        }       
    }


    /**
     * If nothing throws then allNodePools has a map of all node pool files to read and the nodepool props file to attach them to
     * @param p
     */
    void checkForDuplicatePoolFiles()
    		throws IllegalConfigurationException
    {
        for ( DuccProperties dp : nodepools.values() ) {
            String npfile = dp.getProperty("nodefile"); 
            if ( poolsByNodefile.containsKey(npfile) ) {
                throw new IllegalConfigurationException("Duplicate nodepool file reference to " + npfile + " from " + dp.getProperty("name") + " not allowed "
                                                        + " first reference was from " + poolsByNodefile.get(npfile));
                
            }
            if ( npfile != null ) {                  // pools are not required to have nodes associated, e.g. --default--
                poolsByNodefile.put(npfile, dp);
            }            
        }
    }

    void checkForMissingNodeFile()
    	throws IllegalConfigurationException
    {
        List<String> missing = new ArrayList<String>();
        for ( DuccProperties dp : nodepools.values() ) {
            if ( ! dp.containsKey("nodefile") ) {
                missing.add(dp.getProperty("name"));          // remember, for possible exception below

                // No nodefile, assign it ducc_nodes
                // it will crash in a while if this is a conflict
                if ( ducc_nodes != null ) {
                    dp.setProperty("nodefile", ducc_nodes);                
                }
            }
        }
        
        if ( missing.size() > 1 ) {
            StringBuffer sb = new StringBuffer("Multiple nodepools with no associated node file, not allowed: ");
            for (String s : missing) {
                sb.append(" ");
                sb.append(s);
            }
            throw new IllegalConfigurationException(sb.toString());
        }
    }

    void checkForCycles()
    		throws IllegalConfigurationException
    {
        Map<String, String> visited = new HashMap<String, String>();
        List<String>        pools   = new ArrayList<String>();

        for ( DuccProperties dp : nodepools.values() ) {
            visited.clear();
            pools.clear();

            DuccProperties current = dp;
            String currentName = current.getProperty("name");

            while ( current != null ) {
                if ( visited.containsKey(currentName) ) {
                    StringBuffer sb = new StringBuffer();
                    for ( String s : pools ) {
                        sb.append(s);
                        sb.append(" <-- ");
                    }
                    sb.append(currentName);
                    throw new IllegalConfigurationException("Nodepool cycle detected: " + sb.toString());
                } else {
                    visited.put(currentName, currentName);
                    pools.add(currentName);
                    currentName = current.getProperty("parent");
                    if ( currentName != null ) {
                        current = (DuccProperties) nodepools.get(currentName);
                    } else {
                        current = null;
                    }
                }
            } 
        }
    }

    /**
     * Make sure any classes specified in the user registry exist and specify a number.
     * Expected format: 
     *     max_allotment.classname = number
     *
     * UIMA-4275
     */
    void verifyUserLimits()
        throws IllegalConfigurationException
    {
        for (Object o : usermap.keySet() ) {
            DuccProperties dp = usermap.get(o);
            for ( Object l : dp.keySet() ) {

                if ( defaultUser.containsKey(l) ) continue;

                String k = (String) l;
                String val = ((String) dp.get(k)).trim();
                if ( k.indexOf(".") <= 0 ) {
                    throw new IllegalConfigurationException("User " + o + ": allotment incorrectly specified, cannot determine class. " + k + " = " + val);
                }
                String[] tmp = k.split("\\.");
                if ( ! clmap.containsKey(tmp[1]) ) {
                    throw new IllegalConfigurationException("User " + o + ": allotment incorrectly specified, class not defined. " + k + " = " + val);
                }
                try { 
                    Integer.parseInt(val);
                } catch ( NumberFormatException e ) {
                    throw new IllegalConfigurationException("User " + o + ": allotment incorrectly specified, value not a number. " + k + " = " + val);
                }
            }
        }
    }

    /**
     * Read all the node pool files recursively down from the properties file (p) and
     * create a map node -> nodepoolname checking for duplicates.
     * @param p      Properties file representing a nodepool
     * @param domain Default network domain
     */
    void readNpNodes(String domain)
    		throws IllegalConfigurationException
    {

        checkForMissingNodeFile();                       // only one nodepool may be missing its node file
                                                         //    also fills in default nodefile if needed
        checkForDuplicatePoolFiles();                    // only one pool may reference any single file
            
        // if we get here without crash the node pool files are not inconsistent
        for ( String k : poolsByNodefile.keySet() ) {
            readNodepoolNodes(k, (DuccProperties) poolsByNodefile.get(k), domain);
        }
        // TODO: Test above procedures
        //       Assign ducc.nodes to the one allowable top level np with no pool file

    }

    /**
     * Read the complete node configuration as defined in.  Intended for use from command line, not
     * usually elsewhere.
     */
    public void fullValidation(String global_nodefile)
        throws IllegalConfigurationException
    {
        //global_nodefile = resolve(global_nodefile);
        //if ( allNodefiles.containsKey(global_nodefile) ) return;           // already passed if is's there and we're here
        //readNodepoolFile(global_nodefile, defaultDomain, true);            // will throw if there's an issue
    }

    public DuccProperties getDefaultFairShareClass()
    {
        return fairShareDefault;
    }

    public DuccProperties getDefaultFixedClass()
    {
        return fixedDefault;
    }

    public DuccProperties getDefaultReserveClass()
    {
        return reserveDefault;
    }

    public int getShareQuantum(String classname)
        throws IllegalConfigurationException
    {
        // to find the quantum for a class - 
        // -- look up the class to get the nodepool name
        // -- look in the nodepool for the quantum
        // must throw on invalid classname which could be a tupo on the part of whoever calls this
        DuccProperties props = clmap.get(classname);
        if ( props == null ) {
            throw new IllegalConfigurationException("Class " + classname + " is not configured.");
        }
        
        String npname = props.getProperty("nodepool");
        DuccProperties np = nodepools.get(npname);
        return Integer.parseInt(np.getProperty("share-quantum"));        
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

    // UIMA-4142 return domain in API
    public String getDefaultDomain()
    {
        return defaultDomain;
    }

    public Map<String, DuccProperties> getClasses()
    {
        return clmap;
    }

    public Map<String, DuccProperties> getUsers()
    {
    		return usermap;
    }
    
    public void readConfiguration()
        throws FileNotFoundException, 
        IOException,
        IllegalConfigurationException
    {
        if ( ducc_home == null ) {
            throw new IllegalConfigurationException("DUCC_HOME must be defined as a system property.");
        }
        defaultDomain = getDomainName();

        try {
            String tconfig_file_name = resolve(config_file_name);
            if ( tconfig_file_name == null ) {
                throw new IllegalConfigurationException("File " + config_file_name + " does not exist.");
            }
            config_file_name = tconfig_file_name;
            in = new BufferedReader(new FileReader(config_file_name));
            
            parseStanzas();
            doClassInheritance();
            connectNodepools();
            readNpNodes(defaultDomain);
            checkForCycles();
        } finally {
            if ( in != null ) {
                try { in.close();
                } catch (IOException e) {
                    // nothing ... who cares if we got this far, and what can we do anyway ?
                }
            }
        }

        //for (DuccProperties p : independentNodepools) {      // walk the tree and read the node files
        //    readNpNodes(p, defaultDomain);
        //}

        String msg = "";
        String sep = "";
        if ( fairShareExists && (fairShareDefault == null) ) {
            msg = "Definition for Default FAIR_SHARE is missing.";
            sep = "\n";
        }

        if ( fixedExists && (fixedDefault == null) ) {
            msg = msg + sep + "Definition for Default FIXED_SHARE class is missing.";
            sep = "\n";
        }

        if ( reserveExists && (reserveDefault == null) ) {
            msg = msg + sep + "Definition for Default RESERVE class is missing.";
        }
        if ( !msg.equals("") ) {
            throw new IllegalConfigurationException(msg);
        }


        // UIMA-4275 if classes are ok do the user registry, if it exists
        try {
            ducc_users = resolve(ducc_users);
            if ( ducc_users != null ) {
                in = new BufferedReader(new FileReader(ducc_users));
                parseStanzas();
                verifyUserLimits();    // insure the specified classes exist
            }
        } finally {
            if ( in != null ) {
                try { in.close();
                } catch (IOException e) {
                    // nothing ... who cares if we got this far, and what can we do anyway ?
                }
            }
        }

    }

    String formatNodes(Map<String, String> nodes, int indent)
    {
        
        int MAX_WIDTH = 100;
        int cur_width = indent;
        StringBuffer sb = new StringBuffer();

        String leader = String.format("%" + indent + "s", " ");
        if ( nodes == null || nodes.size() == 0 ) return leader + "<None>";

        sb.append(leader);

        for ( String s : nodes.keySet() ) {
            if ( s.indexOf(".") >= 0 ) continue; // skip domains to make it more readable
            if ( cur_width + s.length() + 1 > MAX_WIDTH) {
                sb.append("\n");
                sb.append(leader);
                cur_width = indent;
            } 
            sb.append(s);
            sb.append(" ");
            cur_width += s.length() + 1;
        }
        return sb.toString();
    }
    
    void printNodepool(DuccProperties p, String indent)
    {
        String methodName = "printNodepool";

        logInfo(methodName, indent + "Nodepool " + p.getProperty("name"));
        logInfo(methodName, indent + "   Scheduling quantum: " + p.getProperty("share-quantum"));
        logInfo(methodName, indent + "   Search Order: " + p.getProperty("search-order"));
        String nodefile = p.getProperty("nodefile");
        String nfheader = "   Node File: ";
        logInfo(methodName, indent + nfheader + (nodefile == null ? "None" : nodefile));
        @SuppressWarnings("unchecked")
        Map<String, String> nodes = (Map<String, String>) p.get("nodes");
        logInfo(methodName, formatNodes(nodes, indent.length() + nfheader.length()));
        
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

    void printDeprecatedProperty(String k, Object v, String msg)
    {
        String methodName = "printProperty";
        if ( v == null ) return;  // ignore non-existant properties
        logInfo(methodName, String.format("   %-20s: Deprecated property - %s", k, msg));
    }

    /**
     * Print class values in controlled order and format, not to be confused
     * with what you'd get just iterating the map.
     */
    void printClass(DuccProperties cl)
    {        
        String methodName = "printClass";
        logInfo(methodName, "Class " +           cl.get("name"));
        printProperty("Policy",                  cl.get("policy"));
        printProperty("Nodepool",                cl.get("nodepool"));
        printProperty("Priority",                cl.get("priority"));
        printProperty("Weight",                  cl.get("weight"));
        printProperty("Debug",                   cl.get("debug"));
        printProperty("Cap",                     cl.get("cap"));
        printProperty("Expand By Doubling",      cl.get("expand-by-doubling"));
        printProperty("Initialization Cap",      cl.get("initialization-cap"));
        printProperty("Use Prediction",          cl.get("use-prediction"));
        printProperty("Prediction Fudge",        cl.get("prediction-fudge"));
        printDeprecatedProperty("Max Processes", cl.get("max-processes"), "IGNORED Use max-allotment = [mem in GB] instead.");
        printDeprecatedProperty("Max Machines" , cl.get("max-machines") , "IGNORED Use max-allotment = [mem in GB] instead.");
        printProperty("Max Allotment",           cl.get("max-allotment"));
        printProperty("Enforce Memory Size",     cl.get("enforce"));
        printProperty("Authorized Users"   ,     cl.get("users"));

        logInfo(methodName, "");
    }

    void printUser(DuccProperties cl)
    {        
        String methodName = "printUser";
        logInfo(methodName, "User "  +      cl.get("name"));

        for (Object o : cl.keySet() ) {
        		String k = (String) o;
            if ( k.startsWith("max-allotment") ) {
                printProperty(k, cl.get(k));
            }
        }

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

        DuccProperties[] user_set = usermap.values().toArray(new DuccProperties[usermap.size()]);
        Arrays.sort(user_set, new UserSorter());
        for ( DuccProperties p : user_set ) {
            printUser(p);
        }
    }

    static void usage(String msg)
    {
        if ( msg != null ) {
            System.out.println(msg);
            System.out.println("");
        }
        System.out.println("Usage:");
        System.out.println("    NodeConfiguration [-c class-definitions] [-n nodefile] [-u userfile] [-p] <configfile>");
        System.out.println("Where:");
        System.out.println("    -c <class-definitions> is the class definition file, usually ducc.classes.");
        System.out.println("    -n <nodefile> is nodefile used with tye system, defaults to ducc.nodes");
        System.out.println("    -u <userfile> is the user registry.");
        System.out.println("    -p Prints the parsed configuration, for verification.");
        System.out.println("    -? show this help.");
        System.out.println("");
        System.exit(1);
    }

    /**
     * Testing and verification of the file
     */
    public static void main(String[] args) 
    {

        boolean doprint = false;
        String nodefile = null;
        String userfile = null;
        String config = null;

        int i = 0;

        // (simplistic, expects to be called from a script that does rigorous argument checking
        //              and setup)
        for ( i = 0; i < args.length; i++ ) {
            if ( args[i].equals("-p") ) {
                doprint = true;
                continue;
            }

            if ( args[i].equals("-n") ) {
                nodefile = args[i+1];
                i++;
                continue;
            }

            if ( args[i].equals("-u") ) {
                userfile = args[i+1];
                i++;
                continue;
            }

            if ( args[i].equals("-c") ) {
                config = args[i+1];
                i++;
                continue;
            }

            
            if ( args[i].equals("-?") ) {
                usage(null);
            }
            
        }
            
        if ( config == null ) {
            usage("Class configuration file not specified.");
        }

        if ( nodefile == null ) {
            usage("Nodefile not specified.");
        }

        NodeConfiguration nc = new NodeConfiguration(config, nodefile, userfile, null);

        int rc = 0;
        try {
            nc.readConfiguration();                        // if it doesn't crash it must have worked

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

    static public class UserSorter
        implements Comparator<DuccProperties>
    {
        public int compare(DuccProperties u1, DuccProperties u2)
        {
            String n1 = u1.getProperty("name");
            String n2 = u2.getProperty("name");
            return n1.compareTo(n2);
        }
    }

}

