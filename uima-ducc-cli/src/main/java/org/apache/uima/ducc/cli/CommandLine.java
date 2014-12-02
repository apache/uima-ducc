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
package org.apache.uima.ducc.cli;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CommandLine
{
    private String[] args;                              // incoming args from java command line
    private IUiOption[] opts;                           // options defined for this command

    private Map<String, IUiOption> name_to_option;      // name to IUiOpton map
    private Map<IUiOption, String> option_to_value;     // UiOption to value map
    private Map<IUiOption, IUiOption> legal_options;
    private Properties properties;

    private int help_width = 100;                           // justify help strings to about this

    /**
     * Construct a CommandLine from incoming strings and the legal options.
     * 
     * @param args
     * @param opts
     */
    public CommandLine(String[] args, IUiOption[] opts)
    {
        this(args, opts, null);
    }

    /**
     * Construct a CommandLine from the incoming strings, the legal options, and a
     * properties file.  If the properties file is supplied, it is used to fill in
     * options if not otherwise on the command line.  The command-line strings always
     * take precedence.
     * 
     * @param args
     * @param opts
     * @param props
     */
    public CommandLine(String[] args, IUiOption[] opts, Properties props)
    {

        this.args = args;
        this.opts = opts;
        this.properties = props;
        this.name_to_option  = new HashMap<String, IUiOption>();
        this.option_to_value = new HashMap<IUiOption, String>();
        this.legal_options   = new HashMap<IUiOption, IUiOption>();

        for ( IUiOption o : opts ) {
            name_to_option.put(o.pname(), o);              // String --> option mapping
            legal_options.put(o, o);                       // quick lookup for legal options

            if ( o.sname() != null ) {   // if it has a short name, point into the same place
                name_to_option.put(o.sname(), o);
            }
        }
    }

    /**
     * Returns whether the parsesd command line contained the specified option (by IUiOption).
     *
     * @param opt This is the option to test for.
     *
     * @return <b>true</b> if the specified option was found in the parsed command line, <b>false</b> otherwise
     */
    public boolean contains(IUiOption opt)
    {
        // does the command contain this option?
        return option_to_value.containsKey(opt);
    }

    /**
     * Returns whether the parsesd command line contained the specified option (by string name).
     *
     * @param opt This is the option to test for.
     *
     * @return <b>true</b> if the specified option was found in the parsed command line, <b>false</b> otherwise
     */
    public boolean contains(String opt)
    {
        // does the command contain this option?
        IUiOption o = name_to_option.get(opt);      // see if it's known
        if ( o == null ) return false;              // not known
        return contains(o);                         // does it have a value?
    }

    public boolean isOption(String s)
    {
        // is this string a legal option?
        // for use in parsing values that start with - or --
        if      ( s.startsWith("--") ) s = s.substring(2);
        else if ( s.startsWith("-") )  s = s.substring(1);

        return name_to_option.containsKey(s);
    }

    public boolean isOption(IUiOption k)
    {
        // is this a legal option?
        return legal_options.containsKey(k);
    }

    /**
     * Returns the string value parsed from the command for the specified option, or <b>null</b> if the
     * option was not in the command line.
     *
     * @param opt This is the option to look for.
     *
     * @return the parsed value from the command line for the option or <b>null</b> if the option was not in the
     *         command line.
     *
     * @throws IllegalArgumentException if the option was not found in the command line.
     */
    public String get(IUiOption k)
        throws IllegalArgumentException        
    {
        // what was the parsesd value of this opt
        if ( legal_options.containsKey(k) ) {
            return option_to_value.get(k);
        }
        throw new IllegalArgumentException("Option '" + k.pname() + "' is not a legal option.");
    }

    /**
     * Returns the map of all options found in the command, keyed on the corresponding UiOption.
     * @return Map of options found in the command line.
     */
    public Map<IUiOption, String> allOptions()
    {
        return option_to_value;
    }

    public int getInt(IUiOption k)
        throws IllegalArgumentException,
               NumberFormatException
    {
        // Note - get() checks for existance.  parse() would have filled in defaults.
        return Integer.parseInt(get(k));
    }

    public long getLong(IUiOption k)
        throws IllegalArgumentException,
               NumberFormatException
    {
        // Note - get() checks for existance.  parse() would have filled in defaults.
        return Long.parseLong(get(k));
    }
    
    public boolean getBoolean(IUiOption k)
        throws IllegalArgumentException
    {
        // Note - get() checks for existance.  parse() would have filled in defaults.
        // String values for boolean can be confusing because, Language.  So it's useful
        // and friendly to be quite permissive:
        //   Any capitilaztion of true, t, yes, y, 1  --------> true
        //   Any capitilaztion of false, f, no, n, 0  --------> false

        if ( k.noargs() && get(k) == null ) return true;

        String v = get(k).toUpperCase();
        if ( v.equals("TRUE")  || v.equals("T") || v.equals("YES") || v.equals("Y") || v.equals("1") ) return true;
        if ( v.equals("FALSE") || v.equals("F") || v.equals("NO")  || v.equals("N") || v.equals("0") ) return false;
        throw new IllegalArgumentException("Value is not true | false for argument " + k.pname());
    }

    private void add(IUiOption k, String v)
    {
        if ( contains(k) ) {
            throw new IllegalArgumentException("Duplicate argument " + k.pname() + " not allowed.");
        }
        option_to_value.put(k,v);
    }

    private String justify(int leader, String txt)
    {
        int real_width = help_width - leader;
        String blanks = String.format(("%" + leader + "s"), " ");
        
        if ( txt.length() < real_width ) {                // trivial case, the string fits with no splits
            return blanks + txt;
        }
        
        List<String> pieces_parts = new ArrayList<String>();
        int nparts = txt.length() / real_width;
        if ( txt.length() % real_width > 0 ) {
            nparts++;
        }

        for ( int i = 0; i < nparts-1; i++ ) {
            pieces_parts.add(txt.substring(0, real_width));
            txt = txt.substring(real_width);
        }
        pieces_parts.add(txt);

        
        StringBuffer sb = new StringBuffer();
        for ( String s : pieces_parts ) {
            sb.append(blanks);
            sb.append(s);
            sb.append("\n");
        }
        return sb.toString();
    }

    /**
     * Formats the options into a help screen.   
     * @return The formatted help string.
     */
    public String formatHelp(String commandName)
    {
        //
        // Strategy 
        //    - try to keep line length to witnin maybe 100 chars by wrapping
        //    - get width of widest opt to set first column
        //    - then set argname, required, and default
        //    - on a new line do justified description with indent
        //    - on last line show the example with indent
        //

        StringBuffer sb = new StringBuffer();

        sb.append("Usage:\n");
        sb.append("   ");
        sb.append(commandName);
        sb.append(" [options]\n");
        sb.append("Where options are:\n");

        int len = 0;
        for (IUiOption o: opts) {
            
            int namelen = o.pname().length();
            if ( o.sname() != null ) {
                namelen += o.sname().length() + 3; // +1 for -, 1 for space, 1 for comma
            }
            len = Math.max(len, namelen);
        }
        String fmt1 = "%-" + (len) + "s";  // space for -- and another space
        String fmt2 = "%-" + (len+3) + "s";  // A bit more for description and example lines

        for (IUiOption o: opts) {
            sb.append("--");
            String cmd = o.pname();
            if ( o.sname() != null ) {
                cmd = cmd + ", -" + o.sname();
            }
            sb.append(String.format(fmt1, cmd));
            if ( o.argname() != null ) {
                sb.append(" <");
                sb.append(o.argname());
                sb.append(">");
            }
            if ( o.required() ) {
                sb.append(" (required)");
            } 
            if ( o.optargs() ) {
                sb.append(" Default:  ");
                sb.append(o.deflt());
            }
            if ( o.noargs() ) {
                sb.append(" (no arguments)");
            }
            if ( o.description() != null ) {
                sb.append("\n");
                sb.append(justify(len+3,o.description()));
            }
            if ( o.example() != null ) {
                sb.append("\n");
                sb.append(String.format(fmt2, ""));
                sb.append("Example:  ");
                sb.append(o.example());
            }

            sb.append("\n\n");
        }
        return sb.toString();
    }

    public String toString()
    {
        StringBuffer sb = new StringBuffer();
        for ( IUiOption k : option_to_value.keySet() ) {
            sb.append(k.pname());
            sb.append("=");
            sb.append(option_to_value.get(k));
            sb.append(" ");
        }
        return sb.toString();
    }

    /**
     * Make sure the arguments make sense
     */
    public void sanity()
    {
        /**
         * If multiargs is true, 
         *    noargs  MUSt be false
         *    optargs MAY  be true
         * If noargs is true,
         *    multargs MUSt be false
         *    optargs  MUSt be false
         *    default must be empty
         *       NOTE This is clumsy if the desired input comes from property files.
         *            The reason being, Properties extend HashTable which disallows
         *            null values (throws gratuitous NPE).  So we'll make an
         *            assumption that noargs() options are implicitly boolean,
         *            and allow (but not require) any of the reasonable boolean
         *            representations as values for the default:
         *            true/false, t/f, yes/no, y/n, 1/0
         * If optargs is true
         *    multargs MAY be true
         *    noargs   MUST be false
         *    deflt    must be non-null, non-empty-string
         */

        boolean error = false;
        StringBuffer sb = new StringBuffer();
        for ( IUiOption o : opts ) {
            if ( o.multiargs() && o.noargs() ) {
                if ( error ) sb.append("\n");
                sb.append("Option '" );
                sb.append(o.pname()); 
                sb.append("': multiargs() is true but noargs() is also true.");
                error = true;
                continue;
            }
            
            if ( o.noargs() && o.optargs() ) {
                if ( error ) sb.append("\n");
                sb.append("Option '" );
                sb.append(o.pname()); 
                sb.append("': optargs() is true but noargs() is also true.");
                error = true;
                continue;                
            }
        
            if ( o.noargs() && o.deflt() != null ) {
                try {
                    getBoolean(o);                     // if this doesn't throw the value is a boolean, which we allow
                } catch ( IllegalArgumentException e ) {
                    if ( error ) sb.append("\n");
                    sb.append("Option '" );
                    sb.append(o.pname()); 
                    sb.append("': noargs() is true but a non-boolean default is defined.");
                    error = true;
                }
            }

            if ( o.optargs() && ( o.deflt() == null ) ) {
                if ( error ) sb.append("\n");
                sb.append("Option '" );
                sb.append(o.pname()); 
                sb.append("': optargs() is true but no default is provided.");
                error = true;
                continue;                
            }
        }

        if ( error ) {
            throw new IllegalArgumentException(sb.toString());
        }
    }

    private void addProperties()
    {
        // precondition, caller must insure properties exists
        //
        // Similar to the parse loop, only no need to parse for -- strings, just get the (k, v) pair from the properties
        //
        for (String p : properties.stringPropertyNames()) {
            IUiOption opt = name_to_option.get(p);
            if ( opt == null ) {
                throw new IllegalArgumentException("Illegal keyword for this command: " + p);
            }
            if ( ! contains(opt) ) {               // proceed only if we don't have it
                if ( opt.multiargs() ) {
                    // bop along to end, or next '-' collecting the arguments
                    //TODO - note, we don't have any multi-argument options in DUCC right now
                    throw new IllegalArgumentException("multiargs() is not yet implemented.");
                } else {
                    String v = properties.getProperty(p);
                    if ( opt.noargs() ) {                              // must have no arguments
                        if ( v == null ) {
                            add(opt, null);
                        } else {
                            try {
                                getBoolean(opt);                         // throws if not boolean
                                add(opt, v);
                            } catch ( IllegalArgumentException e ) {
                                throw new IllegalArgumentException("Argument " + opt.pname() + ": no value allowed.  Found " + v);
                            }
                        }
                    } else if ( opt.optargs() ) {                      // may or may not have optional arguments
                        // deal with optional argument 
                        if ( v == null ) {
                            add(opt, opt.deflt());                     // sanity checker insures deflt() is non-null
                        } else {
                            add(opt, v);
                        } 
                        
                    } else {    
                        // Pick up a single argument, and next must start with '-', otherwise its an error.                
                        if ( v == null ) {
                            // nope, required argument is missing
                            throw new IllegalArgumentException("Missing required value for argument " + opt.pname());
                        }
                        add(opt, v);
                    }
                }

            } 
            // else nothing, because the command-strings take precedence            
        }
    }

    private void addCommandLine()
    {

        int i = 0;
        String k;
        String v;
        for ( i = 0; i < args.length; i++ ) {
        	
            // constant: at the top of the loop we must be poised at the next '-' string
            //           must throw otherwise
            if ( args[i].startsWith("--") ) {
                k = args[i].substring(2);
            } else if ( args[i].startsWith("-") ) {
                k = args[i].substring(1);
            } else {
                throw new IllegalArgumentException("Unrecognized keywoard:  " + args[i]);
            }
            
            IUiOption opt = name_to_option.get(k);
            if ( opt == null ) {
                throw new IllegalArgumentException("Illegal keyword for this command: " + args[i]);
            }

            if ( opt.multiargs() ) {
                // bop along to end, or next '-' collecting the arguments
                //TODO - note, we don't have any multi-argument options in DUCC right now
                throw new IllegalArgumentException("multiargs() is not yet implemented.");
            } else {
                if ( i+1 < args.length ) {
                    v = args[i+1];
                } else {
                    v = null;
                }
                if ( opt.noargs() ) {                              // must have no arguments
                    if ( v == null || isOption(v) ) {
                        add(opt, null);
                    } else {
                        throw new IllegalArgumentException("Argument " + opt.pname() + ": no value allowed.  Found " + v);
                    }
                } else if ( opt.optargs() ) {                      // may or may not have optional arguments
                    // deal with optional argument 
                    if ( v == null || isOption(v) ) {
                        add(opt, opt.deflt());                     // sanity checker insures deflt() is non-null
                    } else {
                        add(opt, v);
                        i++;
                    } 

                } else {    
                    // Pick up a single argument, and next must be an options, else it's an error
                    if ( v == null || isOption(v) ) {
                        // nope, required argument is missing
                        throw new IllegalArgumentException("Missing required value for argument " + opt.pname());
                    }
                    i++;
                    add(opt, v);
                }
            }
        }
    }

    /**
     * Final verification of the parsed command line.
     * 
     * Checks that all required arguments are present.
     */
    public void verify()
    {
        for (IUiOption o : opts) {
            if ( o.required() && ! contains(o) ) {
                throw new IllegalArgumentException("Missing required argument " + o.pname());
            }
        }
    }

    public void parse()
    {
        sanity();

        if ( args       != null ) addCommandLine();
        if ( properties != null ) addProperties();
    }
        
}
