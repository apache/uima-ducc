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
package org.apache.uima.ducc.cli.test;

import org.apache.uima.ducc.cli.CommandLine;
import org.apache.uima.ducc.cli.IUiOption;

public class TestCommandLine
    extends ATestDriver
{
    int NTESTS = 8;

    enum OptionSet1
        implements IUiOption
    {
        Administrators {                // to test multiargs
            public String  pname()       { return "administrators"; } 
            public boolean multiargs()   { return true; }
            public String  argname()     { return "list of ids"; } 
            public String  description() { return "Blank-delimited list of userids allowed to manage this service."; } 
            public String  example()     { return "bob mary jimbo"; }
            public String  label()       { return name(); }
        },

        Autostart   { 
            public String  pname()       { return "autostart"; } 
            public boolean noargs()      { return true; }
            public String  description() { return "If True, start the service when DUCC starts."; } 
            public String  example()     { return null; }
            public String  label()       { return name(); }
        },

        Debug {                     // to test optional args
            public String  pname()       { return "debug"; }
            public String  argname()     { return "true|false"; }
            public boolean optargs()     { return true; }
            public String  deflt()       { return "true"; }
            public String  description() { return "Enable CLI Debugging messages."; }
            public String  example()     { return null; }
            public String  label()       { return null; }
        },            

        Description {                     // to test longer quoted strings
            public String  pname()       { return "description"; }
            public String  argname()     { return "string"; }
            public String  description() { return "Test description of the argument."; }
        },            

        DriverJvmArgs {                     // to test - in value
            public String  pname()       { return "driver_jvm_args"; }
            public String  argname()     { return "String with jvm arguments"; }
            public String  description() { return "Args passed to the driver jvm."; }
        },            

        ProcessJvmArgs {                   // to test -- in value
            public String  pname()       { return "process_jvm_args"; }
            public String  argname()     { return "String with jvm arguments"; }
            public String  description() { return "Args passed to JP jvm."; }
        },            

        Help {                      // to test no args
            public String  pname()       { return "help"; }
            public String  argname()     { return null; }
            public boolean noargs()      { return true; }
            public String  description() { return "Print this help message.  It actually an astonishing long and uninformative description in order to test the justify part of the help formatter.  Just read Kipling's Ballad of East and West for the first time in years. Have you ever kippled? Ask the gefilte fish."; }
            public String  example()     { return null; }
            public String  label()       { return null; }
        },            

        SchedulingClass { 
            public String pname()       { return "scheduling_class"; }
            public String argname()     { return "scheduling class name"; }
            public String description() { return "The class to run the job in."; }
            public String example()     { return "normal"; }
            public String label()       { return "SchedulingClass"; }
        },            

        Specification { 
            public String pname()       { return "specification"; }
            public String sname()       { return "f"; }
            public boolean required()   { return true; }
            public String argname()     { return "file"; }
            public String description() { return "Properties file comprising the specification, where the keys are names of parameters. Individual parameters take precedence over those specified in properties file, if any."; }
            public String example()     { return null; }
            public String label()       { return name(); }
        },

        WaitForCompletion { 
            public String pname()       { return "wait_for_completion"; }
            public String argname()     { return null; }
            public boolean noargs()     { return true; }
            public String description() { return "Do not exit until job is completed."; }
            public String example()     { return null; }
            public String label()       { return name(); }
        },            


        WorkingDirectory { 
            public String pname()       { return "working_directory"; }
            public String argname()     { return "path"; }
            public String description() { return "The working directory set in each process. Default to current directory."; }
            public String example()     { return null; }
            public String deflt()       { return "."; }
            public String label()       { return "WorkingDirectory"; }
        },            

        ;

        public String argname()    { return null; }
        public boolean multiargs() { return false; } // the option can have >1 arg
        public boolean required()  { return false; } // this option is required
        public String  deflt()     { return null; }  // default, or ""
        public String  label()     { return null; }  // Parameter name for label in web form
        public String  sname()     { return null; }  // short name of option
        public boolean optargs()   { return false; } // is the argument optional?
        public boolean noargs()    { return false; }
        public String example()    { return null; }

        public String makeDesc()
        {
            if ( example() == null ) return description();
            return description() + "\nexample: " + example();
        }
    };

    // deliberately conflicting
    enum OptionSet2
        implements IUiOption
    {
        Administrators {
            public String pname()       { return "administrators"; } 
            public boolean multiargs()  { return true; }
            public boolean noargs()     { return true; }
            public String argname()     { return "list of ids"; } 
            public String description() { return "Blank-delimited list of userids allowed to manage this service."; } 
            public String example()     { return "bob mary jimbo"; }
            public String label()       { return name(); }
        },
        ;

        public String argname()    { return null; }
        public boolean multiargs() { return false; } // the option can have >1 arg
        public boolean required()  { return false; } // this option is required
        public String  deflt()     { return null; }  // default, or ""
        public String  label()     { return null; }  // Parameter name for label in web form
        public String  sname()     { return null; }  // short name of option
        public boolean optargs()   { return false; } // is the argument optional?
        public boolean noargs()    { return false; }

        public String makeDesc()
        {
            if ( example() == null ) return description();
            return description() + "\nexample: " + example();
        }

    }

    // deliberately conflicting
    enum OptionSet3
        implements IUiOption
    {
        Administrators {
            public String pname()       { return "administrators"; } 
            public boolean optargs()    { return true; }
            public boolean noargs()     { return true; }
            public String argname()     { return "list of ids"; } 
            public String description() { return "Blank-delimited list of userids allowed to manage this service."; } 
            public String example()     { return "bob mary jimbo"; }
            public String label()       { return name(); }
        },

        ;

        public String argname()    { return null; }
        public boolean multiargs() { return false; } // the option can have >1 arg
        public boolean required()  { return false; } // this option is required
        public String  deflt()     { return null; }  // default, or ""
        public String  label()     { return null; }  // Parameter name for label in web form
        public String  sname()     { return null; }  // short name of option
        public boolean optargs()   { return false; } // is the argument optional?
        public boolean noargs()    { return false; }

        public String makeDesc()
        {
            if ( example() == null ) return description();
            return description() + "\nexample: " + example();
        }

    }

    // optargs, but no default specified
    enum OptionSet4
        implements IUiOption
    {
        Administrators {
            public String pname()       { return "administrators"; } 
            public boolean optargs()    { return true; }
            public String argname()     { return "list of ids"; } 
            public String description() { return "Blank-delimited list of userids allowed to manage this service."; } 
            public String example()     { return "bob mary jimbo"; }
            public String label()       { return name(); }
        },

        ;

        public String argname()    { return null; }
        public boolean multiargs() { return false; } // the option can have >1 arg
        public boolean required()  { return false; } // this option is required
        public String  deflt()     { return null; }  // default, or ""
        public String  label()     { return null; }  // Parameter name for label in web form
        public String  sname()     { return null; }  // short name of option
        public boolean optargs()   { return false; } // is the argument optional?
        public boolean noargs()    { return false; }

        public String makeDesc()
        {
            if ( example() == null ) return description();
            return description() + "\nexample: " + example();
        }

    }

    // multiargs and optargs
    enum OptionSet5
        implements IUiOption
    {
        Administrators {
            public String pname()       { return "administrators"; } 
            public boolean multiargs()  { return true; }
            public boolean optargs()    { return true; }
            public String deflt()       { return "bob"; }
            public String argname()     { return "list of ids"; } 
            public String description() { return "Blank-delimited list of userids allowed to manage this service."; } 
            public String example()     { return "bob mary jimbo"; }
            public String label()       { return name(); }
        },

        ;

        public String argname()    { return null; }
        public boolean multiargs() { return false; } // the option can have >1 arg
        public boolean required()  { return false; } // this option is required
        public String  deflt()     { return null; }  // default, or ""
        public String  label()     { return null; }  // Parameter name for label in web form
        public String  sname()     { return null; }  // short name of option
        public boolean optargs()   { return false; } // is the argument optional?
        public boolean noargs()    { return false; }

        public String makeDesc()
        {
            if ( example() == null ) return description();
            return description() + "\nexample: " + example();
        }

    }

    // For mixed option-set testing
    enum OptionSet6
        implements IUiOption
    {
        Specification { 
            public String pname()       { return "specification"; }
            public String sname()       { return "f"; }
            public boolean required()   { return true; }
            public String argname()     { return "file"; }
            public String description() { return "Properties file comprising the specification, where the keys are names of parameters. Individual parameters take precedence over those specified in properties file, if any."; }
            public String example()     { return null; }
            public String label()       { return name(); }
        },
        ;

        public String argname()    { return null; }
        public boolean multiargs() { return false; } // the option can have >1 arg
        public boolean required()  { return false; } // this option is required
        public String  deflt()     { return null; }  // default, or ""
        public String  label()     { return null; }  // Parameter name for label in web form
        public String  sname()     { return null; }  // short name of option
        public boolean optargs()   { return false; } // is the argument optional?
        public boolean noargs()    { return false; }

        public String makeDesc()
        {
            if ( example() == null ) return description();
            return description() + "\nexample: " + example();
        }

    }

    // For mixed option-set testing
    enum OptionSet7
        implements IUiOption
    {
        WorkingDirectory { 
            public String pname()       { return "working_directory"; }
            public String argname()     { return "path"; }
            public String description() { return "The working directory set in each process. Default to current directory."; }
            public String example()     { return null; }
            public String deflt()       { return "."; }
            public String label()       { return "WorkingDirectory"; }
        },            
        ;

        public String argname()    { return null; }
        public boolean multiargs() { return false; } // the option can have >1 arg
        public boolean required()  { return false; } // this option is required
        public String  deflt()     { return null; }  // default, or ""
        public String  label()     { return null; }  // Parameter name for label in web form
        public String  sname()     { return null; }  // short name of option
        public boolean optargs()   { return false; } // is the argument optional?
        public boolean noargs()    { return false; }

        public String makeDesc()
        {
            if ( example() == null ) return description();
            return description() + "\nexample: " + example();
        }

    }


    //
    // establish the tests and the order of execution
    //
    public String[] testsToRun()
    {

        return new String[] {
            "Sanity",
            "BasicParse",
            "OptionalArguments",
            "IncompleteOptionSet",
            "MixedOptions",
            "MissingArguments",
            "MultipleOptionSets",
            "HelpGeneration",
            "MultiTokenValue",
        };
    }

    public void testSanity(String tid)
    {
        //
        // Test parser's sanity checker
        //
        String[] args = {
            "--administrators",
        };
        IUiOption[] opts = {
            OptionSet2.Administrators
        };

        CommandLine cl = new CommandLine(args, opts);
        String testid = tid + ": multiargs=T noargs=T";
        try {
            cl.parse();
            fail(testid, "Parse succeeded but should have failed.");
        } catch ( Exception e ) {
            success(testid, "Parse failed as expected", e.getMessage());
        }        
        

        opts[0] = OptionSet3.Administrators;        
        cl = new CommandLine(args, opts);
        testid = tid + ": optargs=T noargs=T";
        try {
            cl.parse();
            fail(testid, "Parse succeeded but should have failed.");
        } catch ( Exception e ) {
            success(testid, "Parse failed as expected", e.getMessage());
        }        
        
        opts[0] = OptionSet4.Administrators;        
        cl = new CommandLine(args, opts);
        testid = tid + ": optargs=T, no default";
        try {
            cl.parse();
            fail(testid, "Parse succeeded but should have failed."); 
        } catch ( Exception e ) {
            success(testid, "Parse failed as expected", e.getMessage());
        }        
        
        opts[0] = OptionSet5.Administrators;        
        cl = new CommandLine(args, opts);
        testid = tid + ": optargs=T, multiargs=T, fetch deflt";
        try {
            cl.parse();
            success(testid, "Parse succeeded.");
            if ( cl.get(opts[0]).equals("bob")) {
                success(testid, "Default of 'bob' correctly found.");
            } else {
                fail(testid, "Did not fetch default. Expeced 'bob', found", cl.get(opts[0]));
            }
        } catch ( Exception e ) {
            fail(testid, "Parse failed", e.getMessage());
        }        
        
        args = new String[] {
            "--administrators", "mary",
        };
        cl = new CommandLine(args, opts);
        testid = tid + ": optargs=T, multiargs=T, fetch value";
        try {
            cl.parse();
            success(testid, "Parse succeeded.");
            if ( cl.get(opts[0]).equals("mary")) {
                success(testid, "Valu of 'mary' correctly found.");
            } else {
                fail(testid, "Did not fetch default. Expeced 'mary', found", cl.get(opts[0]));
            }
        } catch ( Exception e ) {
            fail(testid, "Parse failed", e.getMessage());
        }        
        
    }

    public void testBasicParse(String testid)
    {
        //
        // Simplest case, everything is paired, everything is provided
        //
        // Also shows proper termination for k,v option
        //
        String[] args = {
            "--specification"    , "1.job",
            "--scheduling_class" , "normal",
            "--working_directory", "/home/bob",
        };
        IUiOption[] opts = {            // must be same order as args so we can verify values easiy
            OptionSet1.Specification,
            OptionSet1.SchedulingClass,
            OptionSet1.WorkingDirectory,
        };

        CommandLine cl = new CommandLine(args, opts);

        try {            
            cl.parse();
            success(testid, "Successful parse");
            System.out.println("Command line: " + cl);    // tests toString()
                
            // in this test, must find everything and values must match
            int i = 1;
            for ( IUiOption o : opts ) {
                if ( cl.contains(o) && args[i].equals(cl.get(o)) ) { 
                    success(testid, "Found option " + o + " = " + cl.get(o));
                } else {                        
                    fail(testid, "Did not find " + o);
                }
                i += 2;
            }
        } catch (Exception e) {
            fail(testid, "Parse failed.", e.getMessage());
        }
        
    }

    public void testOptionalArguments(String tid)
    {
        //
        // Test optional and prohibited parameters
        //
        // debug takes optional parameter - default must exist
        // wait_for_completion takes none - must fail if value is provided
        //
        String[] args = {
            "--debug", "false",
            "--wait_for_completion",
        };
        IUiOption[] opts = {
            OptionSet1.Debug,
            OptionSet1.WaitForCompletion,
        };

        CommandLine cl = new CommandLine(args, opts);
        
        String testid = tid + "1";
        try {            
            cl.parse();
            success(testid, "Successful parse");
            System.out.println("Command line: " + cl);    // tests toString()
                
            // in this test, must find everything
            for ( IUiOption o : opts ) {
                if ( cl.contains(o) ) {
                    success(testid, "Found option " + o + " = " + cl.get(o));
                } else {                        
                    fail(testid, "Did not find " + o);
                }
            }
            if ( !cl.get(OptionSet1.Debug).equals("false") ) {
                fail(testid, "Value for Debug is wwong.  Expected 'false', found", cl.get(OptionSet1.Debug));
            }
        } catch (Exception e) {
            fail(testid, "Parse failed.", e.getMessage());
        }
        
        args = new String[] {
            "--debug",
            "--wait_for_completion", 
        };
        testid = tid + "2";
        cl = new CommandLine(args, opts);

        try {            
            cl.parse();
            success(testid, "Successful parse");
            System.out.println("Command line: " + cl);    // tests toString()
                
            // in this test, must find everything.  default must be filled in for debug, nothing for wait_for_completion
            for ( IUiOption o : opts ) {
                if ( cl.contains(o) ) {
                    success(testid, "Found option " + o + " = " + cl.get(o));
                } else {                        
                    fail(testid, "Did not find " + o);
                }
                if ( o.optargs() && !cl.get(o).equals(o.deflt()) ) {
                    fail(testid, "Incorrect default for", o.pname(), "expected", o.deflt(), "found", cl.get(o));
                }
                if ( o.noargs() && (cl.get(o) != null) ) {
                    fail(testid, "Found non-null value for", o.pname(), "expected null.");
                }
            }
        } catch (Exception e) {
            fail(testid, "Parse failed.", e.toString());
        }
        

        args = new String[] {
            "--debug",
            "--wait_for_completion", "true"
        };
        cl = new CommandLine(args, opts);
        testid = tid + "3";

        try {            
            cl.parse();
            fail(testid, "Parse should have failed on 'wait_for_completion.");
        } catch (Exception e) {
            success(testid, "Parse failed as expected", e.getMessage());
        }        

    }

    public void testIncompleteOptionSet(String testid)
    {
        //
        // Spurious args - the option set is incomplete
        //
        String[] args = {
            "--specification"    , "1.job",
            "--scheduling_class" , "normal",
            "--help",
            "--working_directory", "/home/bob",
            "--debug", "true",
            "--autostart",
        };
        IUiOption[] opts = {
            OptionSet1.Specification,
            OptionSet1.SchedulingClass,
            OptionSet1.WorkingDirectory,
        };

        CommandLine cl = new CommandLine(args, opts);

        try {            
            cl.parse();
            fail(testid, "Incomplete option set should have failed.");
        } catch (Exception e) {
            success(testid, "Parse failed as expected:", e.getMessage());
        }
        
    }

    public void testMixedOptions(String testid)
    {
        //
        // A bunch of options of mixed types.  Expected to succeed
        //
        String[] args = {
            "--specification"    , "1.job",
            "--scheduling_class" , "normal",
            "--working_directory", "/home/bob",
            "--debug", "false",
            "--help",
            "--autostart",
        };
        IUiOption[] opts = { 
            OptionSet1.Specification,
            OptionSet1.SchedulingClass,
            OptionSet1.Help,
            OptionSet1.WorkingDirectory,
            OptionSet1.Debug,
            OptionSet1.Autostart,
        };

        CommandLine cl = new CommandLine(args, opts);

        try {            
            cl.parse();
            success(testid, "Incomplete option set");
            System.out.println("Command line: " + cl);    // tests toString()
                
            String expected = null;
            // in this test, must find everything
            for ( IUiOption o : opts ) {
                if ( cl.contains(o) ) {
                     // insure correct stuff is parsed in
                    switch ( (OptionSet1) o ) {
                        case Specification:
                            expected = "1.job";
                            break;
                        case SchedulingClass:
                            expected = "normal";
                            break;
                        case Help:
                            expected = null;
                            break;
                        case WorkingDirectory:
                            expected = "/home/bob";
                            break;
                        case Debug:
                            expected = "false";
                            break;
                        case Autostart:
                            expected = null;
                            break;
                    }
                    if ( expected == null && expected == cl.get(o) ) {
                        success(testid, "Received expected null for", o.pname());
                    } else if ( expected.equals(cl.get(o)) ) {
                        success(testid, "Received expected", expected, "for", o.pname());
                    } else {
                        fail(testid, "Invalid parse, expected", expected, "and received", cl.get(o));
                    }
                } else {                        
                    fail(testid, "Did not find " + o);
                }
            }
        } catch (Exception e) {
            fail(testid, "Parse failed.", e.getMessage());
        }
        
    }


    public void testMissingArguments(String testid)
    {
        // 
        // Argument 'specification' is required but missing
        //
        String[] args = {
            "--scheduling_class" , "normal",
            "--help",
            "--working_directory", "/home/bob",
            "--debug", "true",
            "--autostart",
        };
        IUiOption[] opts = {
            OptionSet1.Help,
            OptionSet1.Debug,
            OptionSet1.Autostart,
            OptionSet1.SchedulingClass,
            OptionSet1.Specification,
            OptionSet1.WorkingDirectory,
        };

        CommandLine cl = new CommandLine(args, opts);

        try {            
            cl.parse();
            fail(testid, "Incomplete option set should have failed but didn't");

        } catch (Exception e) {
            success(testid, "Parse failed.", e.getMessage());
        }
        
    }

    public void testMultipleOptionSets(String testid)
    {
        // 
        // Arguments from mixed option sets
        //
        String[] args = {
            "--scheduling_class" , "normal",
            "--help",
            "--specification", "1.job",
            "--working_directory", "/home/bob",
            "--debug", "true",
            "--autostart",
        };

        IUiOption[] opts = {
            OptionSet1.Help,
            OptionSet1.Debug,
            OptionSet1.Autostart,
            OptionSet1.SchedulingClass,
            OptionSet6.Specification,
            OptionSet7.WorkingDirectory,
        };

        CommandLine cl = new CommandLine(args, opts);

        try {            
            cl.parse();
            success(testid, "Command line:", cl.toString());
        } catch (Exception e) {
            fail(testid, "Parse with multiple option sets failed.", e.getMessage());
        }
        
    }

    public void testHelpGeneration(String testid)
    {
        // 
        // Auto-help generation.
        // Unclear how to verify so as long as it doesn't crash we'll call it ok.
        //
        IUiOption[] opts = {
            OptionSet1.Administrators,
            OptionSet1.Autostart,
            OptionSet1.Debug,
            OptionSet1.Help,
            OptionSet1.SchedulingClass,
            OptionSet1.Specification,
            OptionSet1.WaitForCompletion,
            OptionSet1.WorkingDirectory,
        };

        CommandLine cl = new CommandLine(null, opts);

        try {            
            success(testid, "Help Generation:\n", cl.formatHelp(this.getClass().getName()));
        } catch (Exception e) {
            fail(testid, "Help generation.");
        }
        
    }

    public void testMultiTokenValue(String tid)
    {
        IUiOption[] opts = {
            OptionSet1.Description,
            OptionSet1.DriverJvmArgs,
            OptionSet1.ProcessJvmArgs,
            OptionSet1.WorkingDirectory,
        };

        // 
        // arguments that start with - or --
        //
        String[] args;
        String testid;
        CommandLine cl;

        args = new String[]{
            "--description", "This is a multitoken description.",    // multitoken value
        };
        testid = tid + " basic";
        cl = new CommandLine(args, opts);
        try {            
            cl.parse();
            success(testid, "Command line:", cl.toString());
        } catch (Exception e) {
            fail(testid, "Multitoken description.", e.getMessage());
        }
 
        args = new String[]{
            "--driver_jvm_args", "-Xmx",                 // one -
        };
        testid = tid + " Args with 1 -.";
        cl = new CommandLine(args, opts);
        try {            
            cl.parse();
            success(testid, "Command line:", cl.toString());
        } catch (Exception e) {
            fail(testid, "Parse args that start with - failed.", e.getMessage());
        }
 
        args = new String[] {
            "--driver_jvm_args", "--Xmx",                // two --
        };
        testid = tid + " Args with 2 --.";
        cl = new CommandLine(args, opts);
        try {            
            cl.parse();
            success(testid, "Command line:", cl.toString());
        } catch (Exception e) {
            fail(testid, "Parse args that start with -- failed.", e.getMessage());
        }
 
        args = new String[] {
            "--driver_jvm_args", "--description",        // it's a keyword, fail
        };
        testid = tid + " Args which are also keywords";
        cl = new CommandLine(args, opts);
        try {            
            cl.parse();
            fail(testid, "Parse arg that's valid keyword succeeded, should have failed.", cl.toString());
        } catch (Exception e) {
            success(testid, "Parse arg that's a keyword failed correctly", e.getMessage());
        }
        
    }

    public static void main(String[] args)
    {
        try {
            TestCommandLine tester = new TestCommandLine();
            tester.runTests();
        } catch ( Exception e ) {
            e.printStackTrace();
        }
    }
}
