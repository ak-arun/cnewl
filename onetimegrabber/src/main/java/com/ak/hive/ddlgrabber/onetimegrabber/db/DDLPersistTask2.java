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
import com.google.common.collect.Iterables;



public class DDLPersistTask2 implements Runnable{
	
	private static final Logger LOG = LoggerFactory.getLogger(DDLPersistTask2.class);
	
	public void run() {
		persist();
	}
	
	private List<DDLObject> ddls;
	private DAO dao;
	private List<DDLObject> ddlsProcessed;
	private Connection connectionHive;
	private String postGresTable;
	private CountDownLatch latch;
	private DBConfig dbConfig;
	private DBConfig destConf;
	private Connection connectionDest;
	
	public DDLPersistTask2(List<DDLObject> ddls, DBConfig dbConfig,DBConfig destConf ,String postGresTable, CountDownLatch latch ) {
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
			
			Iterable<List<DDLObject>> partitions = Iterables.partition(ddls, 100);
			for(List<DDLObject> obj : partitions){
				String query="";
				for(DDLObject o : obj){
					query = query+"show create table "+o.getDatabaseName()+"."+o.getTableName()+";select \"this is an eol\";";
				}
				List<String> ddlList = dao.getDDLList(connectionHive, query);
				int index = 0;
				for(DDLObject o : obj){
					o.setDdl(ddlList.get(index).replace("\n", " ").replace("\r", " ").replaceAll(" +", " ").trim());
					ddlsProcessed.add(o);
					index+=1;
				}
			}
			int index =0;
			for(DDLObject o : ddlsProcessed){
				System.out.println(o);
				index+=1;
				if(index==10){break;}
			}
			System.out.println(ddlsProcessed.size());
			
			//dao.executeInsert(connectionDest, ddlsProcessed, postGresTable);
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
