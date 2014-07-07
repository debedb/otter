package com.enremmeta.otter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.enremmeta.otter.entity.Dataset;
import com.enremmeta.otter.entity.DatasetColumn;

public class Impala {

	private Connection con;

	private Connection getConnection() throws SQLException,
			ClassNotFoundException {
		if (con == null || con.isClosed()) {
			connect();
		}
		return con;
	}

	public void connect() throws SQLException, ClassNotFoundException {
		Config config = Config.getInstance();
		String impalaHost = config.getProperty(Config.PROP_CDH_HOST);
		String url = "jdbc:hive2://" + impalaHost + ":21050/;auth=noSasl";
		Logger.log("Connecting to " + url + "...");
		Class klass = Class.forName("org.apache.hive.jdbc.HiveDriver");
		con = DriverManager.getConnection(url);
		Logger.log("Connected to " + url + ".");
	}

	private Impala() {
		super();
	}

	private static Impala impala = new Impala();

	public static Impala getInstance() {
		return impala;
	}

	public void createDb(String name) throws SQLException, OtterException {
		Connection c = getConnection();
		if (!name.matches("[A-Za-z][A-Za-z0-9]+")) {
			throw new OtterException("Name not allowed: " + name);
		}
		PreparedStatement ps = c.prepareStatement("CREATE DATABASE " + name);
		ps.execute();
		
	}
	
	// TODO partitioning...
	public void addDataset(Dataset ds) throws SQLException,
			ClassNotFoundException {
		String sql = "CREATE EXTERNAL TABLE " + ds.getName() + " ( ";
		String colClause = "";
		for (DatasetColumn col : ds.getColumns()) {
			if (colClause.length() > 0) {
				colClause += ", ";
			}
			colClause += col.getName() + " " + col.getType();
		}
		sql += colClause + ") ";
		sql += "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ";
		sql += "STORED AS TEXTFILE ";
		sql += "LOCATION '/user/x5/" + ds.getName() + "'";
		Connection c = getConnection();
		PreparedStatement ps = c.prepareStatement(sql);
		ps.execute();
		getCount(ds.getName());
	}

	public long getCount(String dsName) throws SQLException, ClassNotFoundException {
		Connection c = getConnection();
		PreparedStatement ps = c.prepareStatement("SELECT COUNT(*) FROM " + dsName);
		ResultSet rs = ps.executeQuery();
		long retval = -1;
		if (rs.next()) {
			 retval = (long)rs.getObject(1);
			Logger.log(dsName + " currently has " +
					+ retval + " rows");
		}
		return retval;
	}
}
