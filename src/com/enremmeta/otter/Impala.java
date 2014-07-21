package com.enremmeta.otter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hive.jdbc.HiveDriver;

import com.enremmeta.otter.entity.Dataset;
import com.enremmeta.otter.entity.DatasetColumn;

public class Impala {

	private Connection con;

	private Connection getConnection() throws SQLException {
		if (con == null || con.isClosed()) {
			connect();
		}
		return con;
	}

	public void connect() throws SQLException {
		Config config = Config.getInstance();
		String impalaHost = config.getProperty(Config.PROP_CDH_HOST);
		String url = "jdbc:hive2://" + impalaHost + ":21050/;auth=noSasl";
		Logger.log("Connecting to " + url + "...");
		Class klass0 = HiveDriver.class;
		Class klass = klass0;
		try {
			klass = Class.forName("org.apache.hive.jdbc.HiveDriver");
		} catch (ClassNotFoundException cnfe) {
			//
		}
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

	public void createDb(String name) throws SQLException, OtterException,
			ClassNotFoundException {
		Connection c = getConnection();
		if (!name.matches("[A-Za-z][A-Za-z0-9]+")) {
			throw new OtterException("Name not allowed: " + name);
		}
		PreparedStatement ps = c.prepareStatement("CREATE DATABASE " + name);
		ps.execute();

	}

	// TODO partitioning...
	/**
	 * Creates a new table in Impala
	 */
	public void addDataset(Dataset ds) throws SQLException,
			ClassNotFoundException {
		String sql = "CREATE EXTERNAL TABLE " + Constants.DB_NAME + "."
				+ ds.getName() + " ( ";
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
		sql += "LOCATION '" + Constants.OTTER_HDFS_PREFIX + ds.getName() + "'";
		Connection c = getConnection();

		PreparedStatement ps = c.prepareStatement(sql);
		ps.execute();
		getCount(Constants.DB_NAME + "." + ds.getName());
	}

	public void refreshTable(String name) throws SQLException {
		Connection c = getConnection();
		String sql = "ALTER TABLE " + Constants.DB_NAME + "." + name + " "
				+ "SET LOCATION '" + Constants.OTTER_HDFS_PREFIX + name + "'";
		PreparedStatement ps = c.prepareStatement(sql);
		ps.execute();
	}

	public void testCleanup() throws OtterException {
		try {
			Connection c = getConnection();
			PreparedStatement ps = c.prepareStatement("DROP TABLE "
					+ Constants.DB_NAME + ".test1");
			ps.execute();
		} catch (SQLException sqle) {
			if (sqle.getMessage().equalsIgnoreCase(
					"AnalysisException: Table does not exist: x5.test1")) {
				// Ignore
			} else {
				throw new OtterException(sqle);
			}
		}
	}

	public Map query(String query) throws SQLException {
		Map map = new HashMap<>();
		Connection c = getConnection();
		List list = new ArrayList<List>();
		PreparedStatement ps = c.prepareStatement("USE " + Constants.DB_NAME);
		ps.execute();
		ps = c.prepareStatement(query);
		ResultSet rs = ps.executeQuery();
		java.sql.ResultSetMetaData rsmd = rs.getMetaData();
		int colCnt = rsmd.getColumnCount();
		List meta = new ArrayList<>();
		for (int i = 1; i <= colCnt; i++) {
			Map colMeta = new HashMap();
			colMeta.put("name", rsmd.getColumnName(i));
			colMeta.put("type", rsmd.getColumnTypeName(i));
			meta.add(colMeta);
		}
		map.put("metadata", meta);
		while (rs.next()) {
			List row = new ArrayList();
			for (int i = 1; i <= colCnt; i++) {
				row.add(rs.getObject(i));
			}
			list.add(row);
		}
		rs.close();
		map.put("data", list);
		return map;
	}

	public List getSample(String tableName, int limit)
			throws SQLException {
		Connection c = getConnection();
		PreparedStatement ps = c.prepareStatement("SELECT * FROM "
				+ Constants.DB_NAME + "." + tableName + " LIMIT " + limit);
		ResultSet rs = ps.executeQuery();
		ResultSetMetaData rsmd = rs.getMetaData();
		int colCnt = rsmd.getColumnCount();
		List retval = new ArrayList();
		while (rs.next()) {
			List row = new ArrayList();
			for (int i = 1; i < colCnt; i++) {
				row.add(rs.getObject(i));
			}
			retval.add(row);
		}
		return retval;
	}

	public long getCount(String dsName) throws SQLException,
			ClassNotFoundException {
		Connection c = getConnection();
		PreparedStatement ps = c.prepareStatement("SELECT COUNT(*) FROM "
				+ dsName);
		ResultSet rs = ps.executeQuery();
		long retval = -1;
		if (rs.next()) {
			retval = (long) rs.getObject(1);
			// Logger.log(dsName + " currently has " + +retval + " rows");
		}
		return retval;
	}
}
