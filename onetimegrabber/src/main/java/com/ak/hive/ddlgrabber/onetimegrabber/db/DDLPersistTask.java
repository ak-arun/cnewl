package com.ak.hive.ddlgrabber.onetimegrabber.db;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ak.hive.ddlgrabber.onetimegrabber.entities.DBConfig;
import com.ak.hive.ddlgrabber.onetimegrabber.entities.DDLObject;
import com.ak.hive.ddlgrabber.onetimegrabber.util.DDLGrabberUtils;



public class DDLPersistTask implements Runnable{
	
	private static final Logger LOG = LoggerFactory.getLogger(DDLPersistTask.class);
	
	public void run() {
		persist();
	}
	
	private List<DDLObject> ddls;
	private DAO dao;
	private List<DDLObject> ddlsProcessed;
	private Connection connectionHive;
	private String ddlString;
	private String postGresTable;
	private CountDownLatch latch;
	private DBConfig dbConfig;
	private DBConfig destConf;
	private Connection connectionDest;
	
	public DDLPersistTask(List<DDLObject> ddls, DBConfig dbConfig,DBConfig destConf ,String postGresTable, CountDownLatch latch ) {
		this.ddls=ddls;
		this.dbConfig=dbConfig;
		this.destConf=destConf;
		this.postGresTable=postGresTable;
		this.latch=latch;
	}

	private void persist() {
		try{
			connectionHive = new ConnectionFactory(dbConfig).getConnectionManager().getConnection();
			connectionDest = new ConnectionFactory(destConf).getConnectionManager().getConnection();
			dao = new DAO();
			ddlsProcessed = new ArrayList<DDLObject>();
			for(DDLObject o : ddls){
				ddlString = dao.getDDL(connectionHive, "`"+o.getDatabaseName()+"."+o.getTableName()+"`");
				o.setDdl(ddlString);
				ddlsProcessed.add(o);
			}
			dao.executeInsert(connectionDest, ddlsProcessed, postGresTable);
			LOG.info("Persisted "+ddlsProcessed.size()+" table ddls to table "+postGresTable);
			connectionHive.close();
			connectionDest.close();
			latch.countDown();
		}catch(Exception e){
			e.printStackTrace();
			LOG.info("Exception persisting ddls to table "+DDLGrabberUtils.getTraceString(e));
		}
		
	}

}
