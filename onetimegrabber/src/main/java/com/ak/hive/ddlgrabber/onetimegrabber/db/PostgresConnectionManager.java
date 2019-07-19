package com.ak.hive.ddlgrabber.onetimegrabber.db;

import com.ak.hive.ddlgrabber.onetimegrabber.entities.DBConfig;

public class PostgresConnectionManager extends ConnectionManager {
	
	public PostgresConnectionManager(DBConfig conf){
		this.conf = conf;
	}
}
