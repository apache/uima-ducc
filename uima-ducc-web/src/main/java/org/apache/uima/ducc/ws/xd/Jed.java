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
package org.apache.uima.ducc.ws.xd;

public class Jed {

	public enum Type {

		DuccJob, Java, File, Exec, Trainer, Sequential, Parallel, ParallelData, SetPhase, Other;

		private static String DUCC_JOB = "DUCC_JOB";
		private static String PARALLEL_DATA = "PARALLEL_DATA";
		private static String SET_PHASE = "SET_PHASE";
		
	    public static Type getEnum(String value) {
	    	Type retVal = Other;
	    	if(value == null) {
	    	}
	    	else if(DUCC_JOB.equalsIgnoreCase(value)) {
	            retVal = DuccJob;
	        }
	        else if(PARALLEL_DATA.equalsIgnoreCase(value)) {
	            retVal = ParallelData;
	        }
	        else if(SET_PHASE.equalsIgnoreCase(value)) {
	            retVal = SetPhase;
	        }
	        else if(DuccJob.name().equalsIgnoreCase(value)) {
	            retVal = DuccJob;
	        }
	        else if(Java.name().equalsIgnoreCase(value)) {
	            retVal = Java;
	        }
	        else if(File.name().equalsIgnoreCase(value)) {
	            retVal = File;
	        }
	        else if(Exec.name().equalsIgnoreCase(value)) {
	            retVal = Exec;
	        }
	        else if(Trainer.name().equalsIgnoreCase(value)) {
	            retVal =  Trainer;
	        }
	        else if(Sequential.name().equalsIgnoreCase(value)) {
	            retVal = Sequential;
	        }
	        else if(ParallelData.name().equalsIgnoreCase(value)) {
	            retVal = ParallelData;
	        }
	        else if(Parallel.name().equalsIgnoreCase(value)) {
	            retVal = Parallel;
	        }
	        else if(SetPhase.name().equalsIgnoreCase(value)) {
	            retVal = SetPhase;
	        }
	        return retVal;
	    }
	    
	    public static boolean isLeaf(Type value) {
	    	boolean retVal = true;
	    	if(value != null) {
	    		switch(value) {
		    	case Parallel:
		    	case ParallelData:
		    	case Sequential:
		    		retVal = false;
		    		break;
		    	default:
		    		break;
		    	}
	    	}
	    	return retVal;
	    }
	    
	    public static boolean isLeaf(String value){
	    	boolean retVal = isLeaf(getEnum(value));
	    	return retVal;
	    }
	}
	
	public enum Status {

		Running, Completed, Done, Canceled, Failed, DependencyFailed, Ignored, Unknown, Other;

		private static String Dependency_Failed = "Dependency-Failed";
		private static String Cancelled = "Cancelled";
		
	    public static Status getEnum(String value) {
	    	Status retVal = Other;
	    	if(value == null) {
	    	}
	    	else if(Cancelled.equalsIgnoreCase(value)) {
	            retVal = Canceled;
	        }
	    	else if(Dependency_Failed.equalsIgnoreCase(value)) {
	            retVal = DependencyFailed;
	        }
	        else if(Running.name().equalsIgnoreCase(value)) {
	            retVal = Running;
	        }
	        else if(Completed.name().equalsIgnoreCase(value)) {
	            retVal = Completed;
	        }
	        else if(Done.name().equalsIgnoreCase(value)) {
	            retVal = Done;
	        }
	        else if(Canceled.name().equalsIgnoreCase(value)) {
	            retVal = Canceled;
	        }
	        else if(Failed.name().equalsIgnoreCase(value)) {
	            retVal = Failed;
	        }
	        else if(DependencyFailed.name().equalsIgnoreCase(value)) {
	            retVal = DependencyFailed;
	        }
	        else if(Ignored.name().equalsIgnoreCase(value)) {
	            retVal = Ignored;
	        }
	        else if(Unknown.name().equalsIgnoreCase(value)) {
	            retVal = Unknown;
	        }
	        return retVal;
	    }
	}

}
