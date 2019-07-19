package com.ak.hive.ddlgrabber.onetimegrabber.db;

import com.ak.hive.ddlgrabber.onetimegrabber.entities.DBConfig;




public class ConnectionFactory {
	
	private static final String DBTYPE_MYSQL = "mysql";
	private static final String DBTYPE_POSTGRES = "postgres";
	private static final String DBTYPE_HIVE = "hive";
	
	private DBConfig conf=null;

	public ConnectionFactory(DBConfig conf){
		this.conf = conf;
	}

	public ConnectionManager getConnectionManager() {
		
		String connectionType = conf.getDbType();

		if (connectionType != null) {

			if (connectionType.equalsIgnoreCase(DBTYPE_MYSQL)) {
				return new MySQLConnectionManager(conf);

			} else if (connectionType
					.equalsIgnoreCase(DBTYPE_POSTGRES)) {
				return new PostgresConnectionManager(conf);

			} else if (connectionType.equalsIgnoreCase(DBTYPE_HIVE)) {
				return new HiveConnectionManager(conf);
			}
		}
		return null;
	}
}
