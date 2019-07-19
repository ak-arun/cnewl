package com.ak.hive.ddlgrabber.onetimegrabber.db;

import java.sql.Connection;
import java.sql.DriverManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import com.ak.hive.ddlgrabber.onetimegrabber.entities.DBConfig;
import com.ak.hive.ddlgrabber.onetimegrabber.exceptions.DBException;

public class HiveConnectionManager extends ConnectionManager {

	Configuration hadoopConf = null;

	public HiveConnectionManager(DBConfig conf) {
		this.conf = conf;
	}

	public Connection getConnection() throws DBException {
		// TODO implement non krb connection

		try {
			hadoopConf = new Configuration();
			hadoopConf.set("hadoop.security.authentication", "Kerberos");
			UserGroupInformation.setConfiguration(hadoopConf);
			UserGroupInformation.loginUserFromKeytab(conf.getPrincipal(),
					conf.getKeytab());
			Class.forName(conf.getDriverClassName());
			connection = DriverManager.getConnection(conf.getConnectString());
		} catch (Exception e) {
			throw new DBException(e);
		}

		return connection;
	}

}
