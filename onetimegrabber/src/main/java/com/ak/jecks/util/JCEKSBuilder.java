package com.ak.jceks.util;

import java.io.FileOutputStream;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;

import javax.crypto.spec.SecretKeySpec;

public class JCEKSBuilder {
public static void main(String[] args) throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
	if(args.length>2){
		String fileName = args[0];
		char[] password = args[1].toCharArray();
		KeyStore ks = JCEKSUtil.loadKeyStore(fileName, password);
		KeyStore.ProtectionParameter protParm	= new KeyStore.PasswordProtection(password);
		int index=2;
		while(index<args.length-1){
			ks.setEntry(args[index], new KeyStore.SecretKeyEntry(new SecretKeySpec(args[index+1].getBytes(), "AES")), protParm);
			index+=2;
		}
		FileOutputStream fos = new FileOutputStream(fileName,true);
		ks.store(fos, password);
	}else{
		System.err.println("Error");
	}
}
}
