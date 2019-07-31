package com.ak.hive.ddlgrabber.onetimegrabber;


import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ak.hive.ddlgrabber.onetimegrabber.db.ConnectionFactory;
import com.ak.hive.ddlgrabber.onetimegrabber.db.DAO;
import com.ak.hive.ddlgrabber.onetimegrabber.db.DDLPersistTask;
import com.ak.hive.ddlgrabber.onetimegrabber.entities.DBConfig;
import com.ak.hive.ddlgrabber.onetimegrabber.entities.DDLObject;
import com.ak.hive.ddlgrabber.onetimegrabber.exceptions.DBException;
import com.ak.jceks.util.JCEKSUtil;
import com.google.common.collect.Iterables;

public class HiveDDLOnetimeGrabber {
	
	private static final Logger LOG = LoggerFactory.getLogger(HiveDDLOnetimeGrabber.class);
	
	List<DDLObject> ddls = null;
	
	public static void main(String[] args) throws DBException, FileNotFoundException, IOException, SQLException, InterruptedException, KeyStoreException, NoSuchAlgorithmException, CertificateException, UnrecoverableKeyException {
		
		Properties properties = new Properties();
		properties.load(new FileReader(new File(args[0])));
		
		DAO dao = new DAO();
		
		KeyStore keyStore = JCEKSUtil.loadKeyStore(properties.getProperty("jceks.file.path"), args[1].toCharArray());
		
		DBConfig confHive = new DBConfig();
		confHive.setPrincipal(properties.getProperty("hive.user.principal"));
		confHive.setKeytab(properties.getProperty("hive.user.keytab"));
		confHive.setConnectString(properties.getProperty("hive.connection.string"));
		confHive.setDriverClassName(properties.getProperty("hive.driver.class"));
		confHive.setDbType("hive");
		
		DBConfig confMetastore = new DBConfig();
		confMetastore.setUserName(properties.getProperty("meta.db.user.name"));
		confMetastore.setPassword(new String(keyStore.getKey("meta.db.user.password", args[1].toCharArray()).getEncoded()));
		confMetastore.setDriverClassName(properties.getProperty("meta.db.driver.class"));
		confMetastore.setConnectString(properties.getProperty("meta.db.connection.string"));
		confMetastore.setDbType(properties.getProperty("meta.db.type"));
		
		DBConfig confDestDb = new DBConfig();
		confDestDb.setUserName(properties.getProperty("ddlstore.db.user.name"));
		confDestDb.setPassword(new String(keyStore.getKey("ddlstore.db.user.password", args[1].toCharArray()).getEncoded()));
		confDestDb.setDriverClassName(properties.getProperty("ddlstore.db.driver.class"));
		confDestDb.setConnectString(properties.getProperty("ddlstore.db.connection.string"));
		confDestDb.setDbType(properties.getProperty("ddlstore.db.type"));
		
		Connection metastoreConnection = new ConnectionFactory(confMetastore).getConnectionManager().getConnection();
		
		List<DDLObject> ddls = dao.getDBAndTables(metastoreConnection, properties.getProperty("meta.query"));
		
		metastoreConnection.close();
		
		LOG.info("Completed fetching "+ddls.size()+" tables from hive metastore");
		
		int threadCount = Integer.parseInt(properties.getProperty("num.executor"));
		int batchCount = Integer.parseInt(properties.getProperty("max.items.per.batch"));
		
		ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadCount);
		Iterable<List<DDLObject>> ddlPartitions = Iterables.partition(ddls, batchCount);
		CountDownLatch latch = new CountDownLatch(Iterables.size(ddlPartitions));
		for(List<DDLObject> ddlObjects : ddlPartitions){
			executor.execute(new DDLPersistTask(ddlObjects, confHive, confDestDb, properties.getProperty("ddlstore.tablename"),latch));
		}
		latch.await();
		executor.shutdown();
		LOG.info("Completed loading ddls for table");
		
	}

	
}
