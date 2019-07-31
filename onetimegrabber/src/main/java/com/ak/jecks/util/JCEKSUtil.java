package com.ak.jceks.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

public class JCEKSUtil {
	
	public static KeyStore loadKeyStore(String fileName, char[] password)
			throws KeyStoreException, IOException, NoSuchAlgorithmException,
			CertificateException {
		KeyStore ks = KeyStore.getInstance("JCEKS");
		
		FileInputStream fis = null;
		try{
			fis = new FileInputStream(fileName);
		}catch(Exception e){
			//ignore
			}
		ks.load(fis, password);
		if(fis!=null){
		fis.close();
		}
		return ks;
	}

}
