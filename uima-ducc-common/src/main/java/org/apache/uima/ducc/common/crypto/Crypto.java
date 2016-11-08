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
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.security.Key;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.RSAPrivateCrtKeySpec;
import java.security.spec.RSAPrivateKeySpec;
import java.security.spec.RSAPublicKeySpec;
import java.util.Set;

import javax.crypto.Cipher;

import org.apache.uima.ducc.common.utils.AlienFile;
import org.apache.uima.ducc.common.utils.DuccPropertiesResolver;
import org.apache.uima.ducc.common.utils.LinuxUtils;

public class Crypto implements ICrypto {
	
	private boolean traditional = false;
	
	private String dirDotDucc = ".ducc";

	private String user;    // Owner of the request - the simulated requester when in test-mode
	private String dirSecurity;
	private String fileDbAccess;
	private String filePvt;
	private String filePub;
	
	private int keySize = 2048;
	private String keyType = "RSA";
	
	private Cipher cipher;
	
	/**
	 * Constructor for requesters
   *  - getSignature returns the the encrypted userid
	 * 
	 * @param user - user making the request
	 * @param create - if true create the public & private keys if missing 
	 * @throws CryptoException
	 */
  public Crypto(String user, boolean create) throws CryptoException {
    init(user, create);
  }
	
  /**
   * Constructor for validators
   *  - use isValid to check that the decrypted signature matches the provided user id
   *  
   * @param user - user claiming to make the request
   * @throws CryptoException
   */
  public Crypto(String user) throws CryptoException {
    init(user, false);
  }
	 
	private void init(String user, boolean createRequest) throws CryptoException {
    
    this.user = user;
    
    // Check if in test mode with simulated users
    String runmode = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_runmode);
    boolean testMode = runmode != null && runmode.equals("Test");
    
    // Get special security home directory if specified
    // In test-mode (single-user) must use the current userid as the simulated user doesn't have a home
    String dirHome = null;
    String ducc_security_home = DuccPropertiesResolver.get(DuccPropertiesResolver.ducc_security_home);
    if (ducc_security_home != null && !ducc_security_home.isEmpty()) {
      String realUser = testMode ? System.getProperty("user.name") : user;
      dirHome = ducc_security_home + File.separator + realUser; 
    }
    
    if (createRequest) {
      // Use the real user home if the special one not specified
      if (dirHome == null) {
        dirHome = System.getProperty("user.home");
      }
      
    } else {
      // When validating a request ....
      // If using the regular home directory get it from the shell as may not start with "/home"
      // In test-mode will always run as the user that started DUCC so use that $HOME
      if (dirHome == null) {
        if (testMode) {
          dirHome = System.getProperty("user.home");
        } else {
          dirHome = LinuxUtils.getUserHome(user);
        }
      }
    }
	  
		dirSecurity = dirHome+File.separator+dirDotDucc;
		fileDbAccess = dirSecurity+File.separator+"db.access";
		filePub = dirSecurity+File.separator+"public.key";
		filePvt = dirSecurity+File.separator+"private.key";
		if (createRequest) {
			createDbAccess();
			createKeys();
			checkKeys();
		}
		try {
			cipher = Cipher.getInstance(keyType);
		}
		catch(Exception e) {
			throw new CryptoException(e);
		}
	}
	
	private boolean isMissingDbAccess() {
		if ((new File(fileDbAccess)).exists()) {
			 return false;
		}
		else {
			return true;
		}
	}
	
	private void createDbAccess() throws CryptoException {
		synchronized(Crypto.class) {
			if(isMissingDbAccess()) {
				mkdir(dirSecurity);
				try {
					File file = new File(fileDbAccess);
					file.createNewFile();
				}
				catch(Exception e) {
					throw new CryptoException(e);
				}
			}
		}
	}
	
	// Check if either file missing (should check that are normal files?)
	private boolean isMissingKeys() {
	  if ((new File(filePvt)).exists() && (new File(filePub)).exists() ) {
	    return false;
	  } else {
	    return true;
	  }
	}
	
	private void createKeys() throws CryptoException {
		try {
			synchronized(Crypto.class) {
				if(isMissingKeys()) {
					mkdir(dirSecurity);
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
					putKeyToFile(filePub, pub.getModulus(), pub.getPublicExponent(), false);
					putKeyToFile(filePvt, pvt.getModulus(), pvt.getPrivateExponent(), true);
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
	
  private void checkKeys() throws CryptoException {
    Path file = Paths.get(filePub);
    if (!Files.exists(file)) {
      throw new CryptoException("File does not exist: " + filePub);
    }
    file = Paths.get(filePvt);
    if (!Files.exists(file)) {
      throw new CryptoException("File does not exist: " + filePvt);
    }
    // Check that the private key file is readable only by the owner
    try {
      // Should be just owner-read
      Set<PosixFilePermission> attrs = Files.getPosixFilePermissions(file);
      if (attrs.size() == 1 && attrs.contains(PosixFilePermission.OWNER_READ)) {
        return;
      }
      System.out.println("Correcting permissions for the private key");
      setPermissions(filePvt, true, false);
      attrs = Files.getPosixFilePermissions(file);
      if (attrs.size() == 1 && attrs.contains(PosixFilePermission.OWNER_READ)) {
        return;
      }
      throw new CryptoException("Unable to correct the invalid permissions for private key file " + filePvt);
    } catch (IOException e) {
      throw new CryptoException(e);
    }
  }
  	
  private void setPermissions(String fileName, boolean pvt, boolean dir) throws CryptoException {
    // Since umask may be anything, turn off r/w access for everybody,
    // make readable by all or just owner, 
    // if a directory make executable by all and writable by owner
    File f = new File(fileName);
    f.setReadable(false, false);
    f.setWritable(false, false);
    f.setReadable(true, pvt);
    f.setWritable(dir, true);
    f.setExecutable(dir, false);
  }
	
	private void mkdir(String dir) throws CryptoException {
		try {
			File file = new File(dir);
			file.mkdirs();
			setPermissions(dirSecurity, false, true);
		}
		catch(Exception e) {
			throw new CryptoException(e);
		}
	}
	
	private void putKeyToFile(String fileName, BigInteger mod, BigInteger exp, boolean pvt) throws CryptoException {
		try {
			ObjectOutputStream oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(fileName)));
			try {
				oos.writeObject(mod);
				oos.writeObject(exp);
				setPermissions(fileName, pvt, false);
			}
			finally {
				oos.close();
			}
		}
		catch(Exception e) {
			throw new CryptoException(e);
		}
	}
	
	private boolean isReadablePublic() {
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
	
	public byte[] getSignature() throws CryptoException {
	  return encrypt(user);
	}
  
	public boolean isValid(byte[] signature) throws CryptoException {
	  String s = (String) decrypt(signature);
	  return user.equals(s);
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
	
	public static void main(String[] args) throws CryptoException {
	  String user = args.length > 1 ? args[1] : System.getProperty("user.name");
	  Crypto cr = new Crypto(user, true);
	  byte[] sig = cr.getSignature();
	  System.out.println("Valid signature: " + cr.isValid(sig));
	}
	
}
