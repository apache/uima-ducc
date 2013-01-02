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
package org.apache.uima.ducc.jd;

public class SynchronizedStats {
    long num;
    double sum;
    double sumsqrs;
    double min;
    double max;
    Object mux;
    public SynchronizedStats() {
        num = 0;
        sum = 0;
        sumsqrs = 0;
        min = Double.MAX_VALUE;
        max = Double.MIN_VALUE;
        mux = new Object();
    }
    public void addValue(double d) {
        synchronized(mux) {
            num++;
            sum += d;
            sumsqrs += d*d;
            if (d < min) {
                min = d;
            }
            if (d > max) {
                max = d;
            }
        }
    }
    public double getMax() {
        synchronized(mux) {
            return max;
        }
    }
    public double getMin() {
        synchronized(mux) {
            return min;
        }
    }
    public double getMean() {
        synchronized(mux) {
            if (num == 0 ) {
                return Double.NaN;
            } else {
                return sum/num;
            }
        }
    }
    public double getStandardDeviation1() {
        synchronized(mux) {
            if ( num == 0 ) {
                return Double.NaN;
            } else {
                return Math.sqrt((num*sumsqrs - sum*sum)/num);
            }
        }
    }
    //s = square root of[(sum of Xsquared -((sum of X)*(sum of X)/N))/(N-1)]
    public double getStandardDeviation() {
        synchronized(mux) {
            if ( num == 0 ) {
                return Double.NaN;
            } else {
            	return Math.sqrt(((sumsqrs-(sum*sum)/num)/(num-1)));
            }
        }
    }
    
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		SynchronizedStats s = new SynchronizedStats();
		s.addValue(1);
		s.addValue(2);
		s.addValue(3);
		System.out.println("max:"+s.getMax());
		System.out.println("min:"+s.getMin());
		System.out.println("avg:"+s.getMean());
		System.out.println("dev:"+s.getStandardDeviation());
	}
}
