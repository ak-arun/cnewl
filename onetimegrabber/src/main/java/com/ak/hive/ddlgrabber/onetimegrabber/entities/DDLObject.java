package com.ak.hive.ddlgrabber.onetimegrabber.entities;


import java.sql.Timestamp;

public class DDLObject {
	
	
	@Override
	public String toString() {
		return "DDLObject [tableName=" + tableName + ", databaseName="
				+ databaseName + ", ddl=" + ddl + ", timestamp=" + timestamp
				+ "]";
	}
	public DDLObject(String tableName, String databaseName, String ddl,
			Long time) {
		super();
		this.tableName = tableName;
		this.databaseName = databaseName;
		this.ddl = ddl;
		this.time=time;
		this.timestamp = new Timestamp(time);
	}
	private String tableName;
	private String databaseName;
	private String ddl;
	private Timestamp timestamp;
	private long time;
	
	public long getTime(){
		return time;
	}
	
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getDatabaseName() {
		return databaseName;
	}
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	public String getDdl() {
		return ddl;
	}
	public void setDdl(String ddl) {
		this.ddl = ddl;
	}
	public Timestamp getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}
	
	
}
