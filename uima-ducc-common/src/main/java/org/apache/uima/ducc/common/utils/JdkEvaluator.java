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
package org.apache.uima.ducc.common.utils;

/*
 * Evaluates java version to determine if it contains JMX security
 * fix described in CVE-2016-3427.
 * 
 */
public class JdkEvaluator {
	public static final int JAVA7=7;
	public static final int JAVA8=8;
	public static final int JAVA9=9;
	public static final int FIXPACK40 = 40;
	public static final int FIXPACK101 = 101;
	public static final int FIXPACK91 = 91;
	public static final int SR3 = 3;
	public static final int SR9 = 9;
	
	public static void main(String[] args) {
		JDKVendor jdkVendor = Vendors.getVendor();
    	System.out.println(jdkVendor.getVendor().name()+" JDK "+jdkVendor.getRuntimeVersion());

		if ( JdkEvaluator.secureJdk(jdkVendor) ) {
			System.out.println(".......... Secure JDK");
		} else {
			System.out.println(".......... Non Secure JDK");
		}
	}
	private JdkEvaluator() {} // static access only
	
	public static boolean secureJdk(JDKVendor vendor) {
		String javaVersion = System.getProperty("java.version");
		
		return vendor.containsSecurityFix(javaVersion);
	}
	public interface JDKVendor {
		public boolean containsSecurityFix(String javaVersion);
		public Vendors.Vendor getVendor();
		public String getRuntimeVersion();
	}
	private static class IBM extends AbstractJDKVendor{
		private Vendors.Vendor vendor = Vendors.Vendor.IBM;
		// secure versions of IBM JDK v 1.7.0.9.40,1.7.1.3.40,1.8.0.3.0

		private static class IbmJdkVersion {
			public static int getMajor(String javaVersion) {
				String [] vs = javaVersion.split("\\.");
				int pos = 0;
				int major = 1;
				// Java 9 comes with a different version string format.
				// It can be just 9, 9-ea or 9.X.X. We just need to 
				// establish if its 9 or higher looking at the major. Any
				// JDK 9 or higher has the jmx fix
				if ( (pos = vs[0].indexOf('-')) > 0 ) {
					major = Integer.parseInt(vs[0].substring(0, pos));
				} else {
					major = Integer.parseInt(vs[0]);
				}
				// versions < 9 include major as a second digit (1.8.1)
				if ( major < JAVA9) {
				  major = Integer.parseInt(vs[1]);
				}
				return major;
			}
			public static int getMinor(String javaVersion) {
				String [] vs = javaVersion.split("\\.");
				int major = getMajor(javaVersion);
				if ( major >= JAVA9) {
					if ( vs.length == 1) {
						return 0;
					} else {
						return Integer.parseInt(vs[1]);
					}
				} else {
					return Integer.parseInt(vs[2]);
				}
			}
		}
		public Vendors.Vendor getVendor() {
			return vendor;
		}

		public boolean containsSecurityFix(String javaVersion) {
        	boolean containsFix = false;
			String runtimeVersion = getRuntimeVersion();
			int major = IbmJdkVersion.getMajor(javaVersion);
			// Java 9 comes with a different version string format.
			// It can be just 9, 9-ea or 9.X.X. We just need to 
			// establish if its 9 or higher looking at the major. Any
			// JDK 9 or higher has the jmx fix
			if ( major >= JAVA9) {
				containsFix = true;
			} else if ( major == JAVA8 ) {
				containsFix = isJava8Fixed(runtimeVersion);
			} else if ( major == JAVA7 ) { 
				containsFix = isJava7Fixed(runtimeVersion, IbmJdkVersion.getMinor(javaVersion));
			}
        	return containsFix;
		}
		private boolean isJava7Fixed(String runtimeVersion, int minor) {
			boolean containsFix = false;
			// if Java 1.7.2+ the jmx fix is there
			if ( minor > 1) {
				return true;
			} 
			int serviceReleaseVersion = getSecurityReleaseVersion(runtimeVersion);
			int fixPackVersion = getFixPackVersion(runtimeVersion);
			
			if ( minor == 1 ) {  // Java 1.7.1
				// check if service release is at least 3 ( SR3)
				if ( serviceReleaseVersion >= SR3) {
					// check if fix pack at least 40
					if ( fixPackVersion >= FIXPACK40) {
						// 1.7.1 JDK equal or above SR3 FP40 we're good
						containsFix = true;
					}
				} 
			} else if ( minor == 0 ) {  // Java 1.7.0
				// check if service release is at least 9 ( SR9)
				if ( serviceReleaseVersion >= SR9) {
					if ( fixPackVersion >= FIXPACK40) {
						// 1.7.0 JDK equal or above SR9 FP40 we're good
						containsFix = true;
					}
				} 
			}
			return containsFix;
		}
		private boolean isJava8Fixed(String runtimeVersion) {
			boolean containsFix = false;
			// Locate Service Release (SR) first. Anything equal or above version 3 is good
			// for IBM Java 8.
			int serviceReleaseVersion = getSecurityReleaseVersion(runtimeVersion);
			if ( serviceReleaseVersion >= 3) {
				int fixPackVersion = getFixPackVersion(runtimeVersion);
				if ( fixPackVersion >= 0) {
					// anything above SR3 is good for Java8
					containsFix = true;
				}
			} 
			return containsFix;
		}
		private int getSecurityReleaseVersion(String runtimeVersion) {
			int start = runtimeVersion.indexOf("(SR");
			if (start > 0 ) {
				String upgradeStringVersion = runtimeVersion.substring(start+1, runtimeVersion.indexOf(')', start));
				String srAsString = ""; // could be {SR[number])
				if ( upgradeStringVersion.indexOf(' ') > -1 ) {
					// this version comes with a fix pack (SR[Number] FP[Number])
					srAsString = upgradeStringVersion.substring(2, upgradeStringVersion.indexOf(' '));
				} else {
					srAsString = upgradeStringVersion.substring(2);
				}
				return Integer.parseInt(srAsString);
			}
			return -1;
		}
		private int getFixPackVersion(String runtimeVersion) {
			int fixPackVersion = -1;
			int start = runtimeVersion.indexOf("(SR");
			if (start > 0 ) {
				//System.out.println(runtimeVersion);
				int p1 = runtimeVersion.indexOf(' ',start);
				int p2 = runtimeVersion.indexOf(')',start);
				String fixPackAsString = 
						runtimeVersion.substring(p1+3 , p2);
				fixPackVersion = Integer.parseInt(fixPackAsString);
			}
			return fixPackVersion;
		}
		
	}
	
	private static class Oracle extends AbstractJDKVendor {
		private Vendors.Vendor vendor = Vendors.Vendor.ORACLE;
		// For JDK 1.7.0, the fix is in fix pack 101
		// For JDK 1.8.0, the fix is in fix pack 91
		
		public Vendors.Vendor getVendor() {
			return vendor;
		}
		
		public String getRuntimeVersion() {
			return System.getProperty("java.runtime.version");
		}
		public boolean containsSecurityFix( String javaVersion) {
        	boolean containsFix = false;
			// 9, 9-ea, 9.0.1
			String [] vs = javaVersion.split("\\.");
        	int pos=0;
        	int major = JdkVersion.getMajor(javaVersion);
        	// versions 9 and above are safe
			if ( major >= JAVA9) {
				containsFix = true;   // good
			} else if ( Integer.parseInt(vs[1]) == JAVA8 ) {
				String fixPack = "";
				int minor = 0;
				//Runtime Version:1.8.0_121-b13
				pos=0;
				if ( (pos = vs[2].indexOf('_')) > 0) {
					minor = Integer.parseInt(vs[2].substring(0, pos));
					// JDK 1.8.1+ contains the fix
					if ( minor > 0) {
						containsFix = true;
					} else {
						if ( vs[2].indexOf('-') > -1) {
							fixPack = vs[2].substring(vs[2].indexOf('_')+1, vs[2].indexOf('-'));
						} else {
							fixPack = vs[2].substring(pos+1);
						}
						
					}
				} 
				if ( !containsFix ) {
					containsFix = isJava8Fixed(fixPack);
				}
			} else if ( Integer.parseInt(vs[1]) == JAVA7 ) {
				String fixPack = "";
				int minor = 0;
				if ( vs[2].indexOf('_') > -1) {
					minor = Integer.parseInt(vs[2].substring(vs[2].indexOf('_')));
					if ( vs[2].indexOf('-') > -1) {
						fixPack = vs[2].substring(vs[2].indexOf('_')+1, vs[2].indexOf('-'));
					} 
				} else {
					minor = Integer.parseInt(vs[2].trim());
				}
				containsFix = isJava7Fixed(minor, fixPack);
			}
        	return containsFix;
		}
		private boolean isJava7Fixed(int minor, String fp) {
			boolean containsFix = false;
			
			// if Java 1.7.1+ the jmx fix is there
			if ( minor > 1) {
				containsFix = true;
			} else if ( minor == 0 ) {  // Java 1.7.0
				int fpNumber = Integer.parseInt(fp);
				if ( fpNumber >= FIXPACK101) {
					// 1.7.0 JDK equal or above 101 we're good
					containsFix = true;
				}
			}
			return containsFix;
		}
		private boolean isJava8Fixed(String fp) {
			boolean containsFix = false;
			// Java 8 must be at least at fix pack 91
			int fpNumber = Integer.parseInt(fp);
			if ( fpNumber >= FIXPACK91) {
				containsFix = true;
			}
			return containsFix;
		}

	}
	private static class OpenJDK extends Oracle {
		private Vendors.Vendor vendor = Vendors.Vendor.OPENJDK;
		@Override
		public Vendors.Vendor getVendor() {
			return vendor;
		}

	}
    public static class Unsupported extends AbstractJDKVendor {
		private Vendors.Vendor vendor = Vendors.Vendor.UNSUPPORTED;

		public Vendors.Vendor getVendor() {
			return vendor;
		}
        public boolean containsSecurityFix(String javaVersion) {
			return false;
		}
	}
    public static abstract class AbstractJDKVendor implements JDKVendor {
    	
		public String getRuntimeVersion() {
			return System.getProperty("java.runtime.version");
		}
		public static class JdkVersion {
			public static int getMajor(String javaVersion) {
				String [] vs = javaVersion.split("\\.");
				int pos = 0;
				int major = 1;
				// Java 9 comes with a different version string format.
				// It can be just 9, 9-ea or 9.X.X. We just need to 
				// establish if its 9 or higher looking at the major. Any
				// JDK 9 or higher has the jmx fix
				if ( (pos = vs[0].indexOf('-')) > 0 ) {
					major = Integer.parseInt(vs[0].substring(0, pos));
				} else {
					major = Integer.parseInt(vs[0]);
				}
				// versions < 9 include major as a second digit (1.8.1)
				if ( major < JAVA9) {
				  major = Integer.parseInt(vs[1]);
				}
				return major;
			}
			public static int getMinor(String javaVersion) {
				String [] vs = javaVersion.split("\\.");
				int major = getMajor(javaVersion);
				if ( major >= JAVA9) {
					if ( vs.length == 1) {
						return 0;
					} else {
						return Integer.parseInt(vs[1]);
					}
				} else {
					return Integer.parseInt(vs[2]);
				}
			}
		}
    }
	public static class Vendors {
		public enum Vendor {IBM, ORACLE, OPENJDK, UNSUPPORTED }
		
		private Vendors() {}  // only static access
		public static JDKVendor getVendor() {
			JDKVendor jdkVendor = null;
			String vendor = System.getProperty("java.vendor");
			if ( vendor.startsWith("IBM")) {
				jdkVendor = new IBM();
			} else if ( vendor.startsWith("Sun") ||  vendor.startsWith("Oracle")) {
				jdkVendor = new Oracle();
			}  else if ( vendor.startsWith("OpenJDK")) {
				jdkVendor = new OpenJDK();
			} else {
				jdkVendor = new Unsupported();
			}
			return jdkVendor;
		}
	}
	
}

