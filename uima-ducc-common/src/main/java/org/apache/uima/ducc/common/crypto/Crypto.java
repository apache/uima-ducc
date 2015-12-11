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
package org.apache.uima.ducc.common.crypto;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.RSAPrivateCrtKeySpec;
import java.security.spec.RSAPrivateKeySpec;
import java.security.spec.RSAPublicKeySpec;

import javax.crypto.Cipher;

import org.apache.uima.ducc.common.RuntimeStreamsConsumer;
import org.apache.uima.ducc.common.utils.AlienFile;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;

public class Crypto implements ICrypto {
	
	private boolean traditional = false;
	
	private String dirDotDucc = ".ducc";
	private String dirDotDuccPermissions  = "0755";
	private String pubFilePermissions = "0755";
	private String pvtFilePermissions = "0700";

	private String securityHome = null;
	
	private String user;
	private String dirUserKeys;
	private String filePvt;
	private String filePub;
	
	private int keySize = 2048;
	private String keyType = "RSA";
	
	private Cipher cipher;
	
	public enum AccessType {
		READER,
		WRITER,
	}
	
	public Crypto(String user, String dirHome) throws CryptoException {
		init(user,dirHome,dirDotDucc,AccessType.WRITER);
	}
	
	public Crypto(String user, String dirHome, AccessType accessType) throws CryptoException {
		init(user,dirHome,dirDotDucc,accessType);
	}
	
	public Crypto(String user, String dirHome, String dirSub) throws CryptoException {
		init(user,dirHome,dirSub,AccessType.WRITER);
	}
	
	public Crypto(String user, String dirHome, String dirSub, AccessType accessType) throws CryptoException {
		init(user,dirHome,dirSub,accessType);
	}
	
	private void resolveSecurityHome(String tgtUser, String dirHome) {
		securityHome = dirHome;
		String ducc_security_home = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_security_home);
		if(ducc_security_home != null) {
			StringBuffer sb = new StringBuffer();
			sb.append(ducc_security_home);
			if(!ducc_security_home.endsWith(File.separator)) {
				sb.append(File.separator);
			}
			sb.append(tgtUser);
			securityHome = sb.toString();
		}
	}
	
	private void init(String tgtUser, String dirHome, String dirSub, AccessType accessType) throws CryptoException {
		resolveSecurityHome(tgtUser, dirHome);
		user = tgtUser;
		dirUserKeys = securityHome+File.separator+dirSub;
		filePub = dirUserKeys+File.separator+"public.key";
		filePvt = dirUserKeys+File.separator+"private.key";
		switch(accessType) {
		case READER:
			break;
		case WRITER:
			createKeys();
			checkKeys();
			break;
		}
		try {
			cipher = Cipher.getInstance(keyType);
		}
		catch(Exception e) {
			throw new CryptoException(e);
		}
	}
	
	public String getPublic() {
		return filePub;
	}
	
	public String getPrivate() {
		return filePvt;
	}
	
	private boolean isMissingKeys() {
		boolean retVal = false;
		try {
			checkFile(filePub);
			checkFile(filePvt);
		}
		catch(Exception e) {
			retVal = true;
		}
		return retVal;
	}
	
	private void createKeys() throws CryptoException {
		try {
			synchronized(Crypto.class) {
				if(isMissingKeys()) {
					mkdir(dirUserKeys, dirDotDuccPermissions);
					KeyPairGenerator kpg = KeyPairGenerator.getInstance(keyType);
					kpg.initialize(keySize);
					KeyPair kp = kpg.genKeyPair();
					KeyFactory keyFactory = KeyFactory.getInstance(keyType);
					RSAPublicKeySpec pub = keyFactory.getKeySpec(kp.getPublic(), RSAPublicKeySpec.class);
					// <IBM JDK does not seem to support RSAPrivateKeySpec.class>
					RSAPrivateKeySpec pvt;
					try {
						pvt = keyFactory.getKeySpec(kp.getPrivate(), RSAPrivateKeySpec.class);
					}
					catch(Exception e) {
						pvt = keyFactory.getKeySpec(kp.getPrivate(), RSAPrivateCrtKeySpec.class);
					}
					// </IBM JDK does not seem to support RSAPrivateKeySpec.class>
					putKeyToFile(filePub, pub.getModulus(), pub.getPublicExponent(),pubFilePermissions);
					putKeyToFile(filePvt, pvt.getModulus(), pvt.getPrivateExponent(),pvtFilePermissions);
				}
			}
		}
		catch(CryptoException e) {
			throw e;
		}
		catch(Exception e) {
			throw new CryptoException(e);
		}
	}
	
	private void checkDir(String fileName) throws CryptoException {
		File file = new File(fileName);
		if(!file.exists()) {
			throw new CryptoException("Directory does not exist: "+fileName);
		}
	}
	
	private void checkFile(String fileName) throws CryptoException {
		File file = new File(fileName);
		if(!file.exists()) {
			throw new CryptoException("File does not exist: "+fileName);
		}
	}
	
	private void checkKeys() throws CryptoException {
		checkDir(dirUserKeys);
		checkFile(filePvt);
		checkFile(filePub);
	}
	
	private void exec(String cmd) throws CryptoException {
		try {
			Process process;
			process = Runtime.getRuntime().exec(cmd);
			RuntimeStreamsConsumer errConsumer = new RuntimeStreamsConsumer(process.getErrorStream(), System.err);
			RuntimeStreamsConsumer outConsumer = new RuntimeStreamsConsumer(process.getErrorStream(), System.out);
			errConsumer.start();
			outConsumer.start();
			process.waitFor();
			errConsumer.join();
			outConsumer.join();
		}
		catch(Exception e) {
			throw new CryptoException(e);
		}
	}
	
	private void chmod(String fileName, String permissions) throws CryptoException {
	  String osName = System.getProperty("os.name");
	  if (osName.startsWith("Windows")) {
	    // Windows is not supported for running
	    // For building, some tests run through this code,
	    //   so we bypass doing the chmod on Windows environments
	    return;
	  }
		try {
			exec("chmod "+permissions+" "+fileName);
		}
		catch(Exception e) {
			throw new CryptoException(e);
		}
	}
	
	private void mkdir(String dir, String permissions) throws CryptoException {
		try {
			File file = new File(dir);
			file.mkdirs();
			chmod(dirUserKeys, permissions);
		}
		catch(Exception e) {
			throw new CryptoException(e);
		}
	}
	
	private void putKeyToFile(String fileName, BigInteger mod, BigInteger exp, String permissions) throws CryptoException {
		try {
			ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(fileName)));
			try {
				oos.writeObject(mod);
				oos.writeObject(exp);
				chmod(fileName, permissions);
			}
			finally {
				oos.close();
			}
		}
		catch(Exception e) {
			throw new CryptoException(e);
		}
	}
	
	public boolean isReadablePrivate() {
		boolean readable = false;
		File file = new File(filePvt);
		readable = file.canRead();
		return readable;
	}
	
	public boolean isReadablePublic() {
		boolean readable = false;
		File file = new File(filePub);
		readable = file.canRead();
		return readable;
	}
	
	private Key getPubicKeyFromFile() throws CryptoException {
		try {
			String fileName = filePub;
			ObjectInputStream ois = null;
			DataInputStream dis = null;
			try {
				if(isReadablePublic()) {
					ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(fileName)));
				}
				else {
					AlienFile alienFile = new AlienFile(user,fileName);
					dis = alienFile.getDataInputStream();
					ois = new ObjectInputStream(new BufferedInputStream(dis));
				}
				BigInteger mod = (BigInteger) ois.readObject();
			    BigInteger exp = (BigInteger) ois.readObject();
			    RSAPublicKeySpec publicKeySpec = new RSAPublicKeySpec(mod, exp);
			    if(traditional) {
			    	KeyFactory keyFactory = KeyFactory.getInstance(keyType);
			    	PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);
			    	return publicKey;
			    }
			    
			    RSAPrivateKeySpec spec = new RSAPrivateKeySpec(publicKeySpec.getModulus(),publicKeySpec.getPublicExponent());
			    Key key = KeyFactory.getInstance("RSA").generatePrivate(spec);
			    
			    return key;
			}
			finally {
				if(ois != null) {
					ois.close();
				}
				if(dis != null) {
					dis.close();
				}
			}
		}
		catch(Throwable t) {
			throw new CryptoException(t);
		}
	}
	
	private Key getPrivateKeyFromFile() throws CryptoException {
		try {
			String fileName = filePvt;
			ObjectInputStream ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(fileName)));
			try {
				BigInteger mod = (BigInteger) ois.readObject();
			    BigInteger exp = (BigInteger) ois.readObject();
			    RSAPrivateKeySpec privateKeySpec = new RSAPrivateKeySpec(mod, exp);
			    
			    if(traditional) {
			    	KeyFactory keyFactory = KeyFactory.getInstance(keyType);
			    	PrivateKey privateKey = keyFactory.generatePrivate(privateKeySpec);
			    	return privateKey;
			    }
			    
			    RSAPublicKeySpec spec = new RSAPublicKeySpec(privateKeySpec.getModulus(),privateKeySpec.getPrivateExponent());
			    Key key = KeyFactory.getInstance("RSA").generatePublic(spec);

			    return key;
			}
			finally {
				ois.close();
			}
		}
		catch(Exception e) {
			throw new CryptoException(e);
		}
	}
	
	private byte[] o2b(Object object) throws CryptoException {
		byte[] byteArray;
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutput oo = new ObjectOutputStream(bos);   
			try {
				oo.writeObject(object);
				byteArray = bos.toByteArray();
			}
			finally {
				oo.close();
				bos.close();
			}
		}
		catch(Exception e) {
			throw new CryptoException(e);
		}
		return byteArray;
	}
	
	private Object b2o(byte[] byteArray) throws CryptoException {
		Object object;
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(byteArray);
			ObjectInput oi = new ObjectInputStream(bis);   
			try {
				object = oi.readObject();
			}
			finally {
				oi.close();
				bis.close();
			}
		}
		catch(Exception e) {
			throw new CryptoException(e);
		}
		return object;
	}
	

	public byte[] encrypt(Object o) throws CryptoException {
		try {
			Key key = getPrivateKeyFromFile();
			cipher.init(Cipher.ENCRYPT_MODE, key);
			return cipher.doFinal(o2b(o));
			}
		catch(Exception e) {
			throw new CryptoException(e);
		}
	}
	

	public Object decrypt(byte[] byteArray) throws CryptoException {
		try {
			Key key = getPubicKeyFromFile();
			cipher.init(Cipher.DECRYPT_MODE, key);
			return b2o(cipher.doFinal(byteArray));
			}
		catch(Exception e) {
			throw new CryptoException(e);
		}
	}
	
}
