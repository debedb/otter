package com.enremmeta.otter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.Properties;

import com.enremmeta.otter.entity.Dataset;
import com.enremmeta.otter.entity.DatasetColumn;

public class OfficeDb {

	private OfficeDb() {
		super();
	}

	private static final OfficeDb odb = new OfficeDb();

	public static OfficeDb getInstance() {
		return odb;
	}

	private Connection con;

	private Connection getConnection() throws SQLException {
		if (con == null || con.isClosed()) {
			connect();
		}
		return con;
	}

	public void connect() throws SQLException {
		Config config = Config.getInstance();
		Properties connectionProps = new Properties();
		connectionProps.put("user", config.getProperty(Config.PROP_MYSQL_USER));
		connectionProps.put("password",
				config.getProperty(Config.PROP_MYSQL_PASS));

		String url = "jdbc:mysql://"
				+ config.getProperty(Config.PROP_MYSQL_HOST) + "/"
				+ config.getProperty(Config.PROP_MYSQL_METADATA_DB);
		Logger.log("Connecting to " + url + "...");
		con = DriverManager.getConnection(url, connectionProps);
		Logger.log("Connected to " + url + ".");
		con.setAutoCommit(false);
	}

	public int addDataset(Dataset ds) throws SQLException {
		Connection c = getConnection();
		try {
			PreparedStatement ps = c.prepareStatement(
					"INSERT INTO dataset (title) VALUES (?)",
					Statement.RETURN_GENERATED_KEYS);
			ps.setObject(1, ds.getName());
			ps.executeUpdate();
			int datasetId = 0;
			ResultSet rs = ps.getGeneratedKeys();
			if (rs.next()) {
				datasetId = rs.getInt(1);
			} else {
				throw new RuntimeException("Did not get generated keys");
			}

			ps = c.prepareStatement(
					"INSERT INTO dataset_property (dataset_id, name, title, type, fmt) VALUES (?, ?, ?, ?, ?)",
					Statement.RETURN_GENERATED_KEYS);
			for (DatasetColumn col : ds.getColumns()) {

				String colName = col.getName();
				String colType = col.getType();
				String fmt = col.getFmt();
				String title = col.getTitle();
				if (title == null || title.length() == 0) {
					title = colName;
				}
				ps.setObject(1, datasetId);
				ps.setObject(2, colName);
				ps.setObject(3, title);
				ps.setObject(4, colType);
				ps.setObject(5, fmt);
				ps.addBatch();
			}
			ps.executeBatch();
			ps.close();
			c.commit();
			return datasetId;
		} catch (Exception sqle) {
			rollback();
			throw sqle;
		} finally {
		}
	}

	public int testCleanup() throws OtterException {
//
//		Connection c;
//		try {
//			c = getConnection();
//		} catch (SQLException e) {
//			throw new OtterException(e);
//		}
//		try {
//			PreparedStatement ps = c
//					.prepareStatement("DELETE FROM dataset WHERE title = 'test1'");
//			int upd = ps.executeUpdate();
//			ps.close();
//			c.commit();
//			return upd;
//		} catch (SQLException sqle) {
//			rollback();
//			throw new OtterException(sqle);
//		} finally {
//		}
		return 0;
	}

	private void rollback() {
		try {
			Connection c = getConnection();
			c.rollback();
		} catch (SQLException sqle) {

		}
	}

	public Dataset getDataset(int datasetId) throws SQLException {
		Connection c = getConnection();
		PreparedStatement ps = c
				.prepareStatement("SELECT us.db_name, up.name, up.type FROM universal_source us JOIN universal_property up ON up.universal_source_id = us.id "
						+ "WHERE us.id = ?");
		ps.setObject(1, datasetId);
		ResultSet rs = ps.executeQuery();
		Dataset retval = null;
		while (rs.next()) {
			if (retval == null) {
				retval = new Dataset();
				retval.setName(rs.getString(1));
			}
			DatasetColumn col = new DatasetColumn();
			col.setName(rs.getString(2));
			col.setType(rs.getString(3));
			//col.setFmt(rs.getString(4));
			retval.getColumns().add(col);
		}
		return retval;
	}
}
