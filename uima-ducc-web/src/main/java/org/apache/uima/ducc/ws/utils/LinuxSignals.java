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
package org.apache.uima.ducc.ws.utils;

import java.util.TreeMap;

public class LinuxSignals {

	public enum Signal {
	    SIGHUP     (Integer.valueOf(1) ,  "Hangup (POSIX)"),
	    SIGINT     (Integer.valueOf(2) ,  "Interrupt (ANSI)"),
	    SIGQUIT    (Integer.valueOf(3) ,  "Quit (POSIX)"),
	    SIGILL     (Integer.valueOf(4) ,  "Illegal instruction (ANSI)"),
	    SIGTRAP    (Integer.valueOf(5) ,  "Trace trap (POSIX)"),
	    SIGABRT    (Integer.valueOf(6) ,  "Abort (ANSI)"),
	    SIGBUS     (Integer.valueOf(7) ,  "BUS error (4.2 BSD)"),
	    SIGFPE     (Integer.valueOf(8) ,  "Floating-point exception (ANSI)"),
	    SIGKILL    (Integer.valueOf(9) ,  "Kill, unblockable (POSIX)"),
	    SIGUSR1    (Integer.valueOf(10),  "User-defined signal 1 (POSIX)"),
	    SIGSEGV    (Integer.valueOf(11),  "Segmentation violation (ANSI)"),
	    SIGUSR2    (Integer.valueOf(12),  "User-defined signal 2 (POSIX)"),
	    SIGPIPE    (Integer.valueOf(13),  "Broken pipe (POSIX)"),
	    SIGALRM    (Integer.valueOf(14),  "Alarm clock (POSIX)"),
	    SIGTERM    (Integer.valueOf(15),  "Termination (ANSI)"),
	    SIGSTKFLT  (Integer.valueOf(16),  "Stack fault"),
	    SIGCHLD    (Integer.valueOf(17),  "Child status has changed (POSIX)"),
	    SIGCONT    (Integer.valueOf(18),  "Continue (POSIX)"),
	    SIGSTOP    (Integer.valueOf(19),  "Stop, unblockable (POSIX)"),
	    SIGTSTP    (Integer.valueOf(20),  "Keyboard stop (POSIX"),
	    SIGTTIN    (Integer.valueOf(21),  "Background read from tty (POSIX)"),
	    SIGTTOU    (Integer.valueOf(22),  "Background write to tty (POSIX)"),
	    SIGURG     (Integer.valueOf(23),  "Urgent condition on socket (4.2 BSD)"),
	    SIGXCPU    (Integer.valueOf(24),  "CPU limit exceeded (4.2 BSD)"),
	    SIGXFSZ    (Integer.valueOf(25),  "File size limit exceeded (4.2 BSD)"),
	    SIGVTALRM  (Integer.valueOf(26),  "Virtual alarm clock (4.2 BSD)"),
	    SIGPROF    (Integer.valueOf(27),  "Profiling alarm clock (4.2 BSD)"),
	    SIGWINCH   (Integer.valueOf(28),  "Window size change (4.3 BSD, Sun)"),
	    SIGIO      (Integer.valueOf(29),  "I/O now possible (4.2 BSD)"),
	    SIGPWR     (Integer.valueOf(30),  "Power failure restart (System V)"),
	    SIGSYS     (Integer.valueOf(31),  "Bad system call"),
	    ;

	    private final int number;
	    private final String description;
	    
	    Signal(int number, String description) {
	        this.number = number;
	        this.description = description;
	    }
	    
	    public int number() { return number; }
	    public String description() { return description; }
	}

    private static int signalUpperLimit = Signal.SIGSYS.number()+1;
    
	private static TreeMap<Integer, Signal> map = createMap();
	
	private static TreeMap<Integer, Signal> createMap() {
		TreeMap<Integer, Signal> signalMap = new TreeMap<Integer, Signal>();
		mapAdd(signalMap, Signal.SIGHUP);
		mapAdd(signalMap, Signal.SIGINT);
		mapAdd(signalMap, Signal.SIGQUIT);
		mapAdd(signalMap, Signal.SIGILL);
		mapAdd(signalMap, Signal.SIGTRAP);
		mapAdd(signalMap, Signal.SIGABRT);
		mapAdd(signalMap, Signal.SIGBUS);
		mapAdd(signalMap, Signal.SIGFPE);
		mapAdd(signalMap, Signal.SIGKILL);
		mapAdd(signalMap, Signal.SIGUSR1);
		mapAdd(signalMap, Signal.SIGSEGV);
		mapAdd(signalMap, Signal.SIGUSR2);
		mapAdd(signalMap, Signal.SIGPIPE);
		mapAdd(signalMap, Signal.SIGALRM);
		mapAdd(signalMap, Signal.SIGTERM);
		mapAdd(signalMap, Signal.SIGSTKFLT);
		mapAdd(signalMap, Signal.SIGCHLD);
		mapAdd(signalMap, Signal.SIGCONT);
		mapAdd(signalMap, Signal.SIGSTOP);
		mapAdd(signalMap, Signal.SIGTSTP);
		mapAdd(signalMap, Signal.SIGTTIN);
		mapAdd(signalMap, Signal.SIGTTOU);
		mapAdd(signalMap, Signal.SIGURG);
		mapAdd(signalMap, Signal.SIGXCPU);
		mapAdd(signalMap, Signal.SIGXFSZ);
		mapAdd(signalMap, Signal.SIGVTALRM);
		mapAdd(signalMap, Signal.SIGPROF);
		mapAdd(signalMap, Signal.SIGWINCH);
		mapAdd(signalMap, Signal.SIGIO);
		mapAdd(signalMap, Signal.SIGPWR);
		mapAdd(signalMap, Signal.SIGSYS);
		return signalMap;
	}
	
	private static void mapAdd(TreeMap<Integer, Signal> signalMap, Signal signal) {
		Integer key = Integer.valueOf(signal.number);
		Signal value = signal;
		signalMap.put(key,value);
	}
	
	private static int boundary_lower = 128;
	private static int boundary_upper = 128+signalUpperLimit;
	
	public static int getValue(int code) {
		return code - boundary_lower;
	}
	
	public static boolean isSignal(int code) {
		return (code > boundary_lower) && (code < boundary_upper);
	}
	
	public static Signal lookup(int code) {
		Signal retVal = null;
		Integer key = Integer.valueOf(code-boundary_lower);
		if(map.containsKey(key)) {
			retVal = map.get(key);
		}
		return retVal;
	}
	
	private static void report(int code) {
		if(isSignal(code)) {
			Signal signal = lookup(code);
			System.out.println("name="+signal.name()+" "+"number="+signal.number()+" "+"description="+signal.description());
		}
		else {
			System.out.println("name="+"<none>"+" "+"number="+code+" "+"description="+"<none>");
		}
	}
	
	public static void main(String[] args) {
		for(int i=1; i < 35; i++) {
			report(i+boundary_lower);
		}
	}
	
}
