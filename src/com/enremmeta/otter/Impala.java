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

import com.enremmeta.otter.entity.Dataset;
import com.enremmeta.otter.entity.DatasetColumn;
import com.enremmeta.otter.entity.Task;
import com.enremmeta.otter.entity.TaskDataSet;
import com.enremmeta.otter.entity.TaskDataSetFilter;
import com.enremmeta.otter.entity.TaskDataSetModifier;
import com.enremmeta.otter.entity.TaskDataSetModifierGroup;
import com.enremmeta.otter.entity.TaskDataSetModifierSort;
import com.enremmeta.otter.entity.TaskDataSetProperty;
import com.enremmeta.otter.entity.messages.Field;

public class Impala {

	/**
	 * Make the type work as Impala type
	 */

	private static String fixType(String type) {
		String retval = type;
		if (type.equals("varchar")) {
			retval = "string";
		} else if (type.equalsIgnoreCase("datetime")
				|| type.equalsIgnoreCase("date")) {
			retval = "timestamp";
		} else if (type.equalsIgnoreCase("enum")) {
			retval = "string";
		}
		Logger.log("Replaced " + type + " with " + retval);
		return retval;
	}

	private Connection con;

	private Config config;

	public Impala() {
		super();
		config = Config.getInstance();
	}

	// TODO partitioning...
	/**
	 * Creates a new table in Impala.
	 */
	public void addDataset(Dataset ds) throws SQLException,
			ClassNotFoundException {
		String sql = "CREATE EXTERNAL TABLE " + getTableName(ds) + " ( ";
		String colClause = "";
		for (DatasetColumn col : ds.getColumns()) {
			if (colClause.length() > 0) {
				colClause += ", ";
			}
			String type = col.getType().toLowerCase();
			type = fixType(type);
			colClause += col.getName() + " " + type;
		}
		sql += colClause + ") ";
		// TODO!!!
		sql += "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ";
		sql += "STORED AS TEXTFILE ";
		sql += "LOCATION '" + config.getOtterHdfsPrefix() + ds.getName() + "'";
		Connection c = getConnection();

		PreparedStatement ps = c.prepareStatement(sql);
		ps.execute();
		getCount(config.getImpalaDbName() + "." + ds.getName());
	}

	public long runWorkflow(WorkflowMetaData wfmd) throws Exception {
		String sql = wfmd.getSql();
		Connection c = getConnection();

		PreparedStatement ps = c.prepareStatement(sql);
		ps.execute();
		long result = getCount(config.getImpalaDbName() + "."
				+ wfmd.getMetaResult().getTableName());
		return result;
	}

	public List<WorkflowMetaData> buildWorkflow(Task task, String workflowId)
			throws OtterException {

		List<WorkflowMetaData> retval = new ArrayList<WorkflowMetaData>();
		String fromClause = "";
		List<String> tables = new ArrayList<String>();
		int tdsCnt = 0;
		for (Long tdsId : task.getDatasets().keySet()) {
			tdsCnt++;
			String outTableName = "task_" + task.getId() + "_wf_" + workflowId
					+ "_ds_" + tdsCnt + "_x_" + System.currentTimeMillis();
			WorkflowMetaData wfmd = new WorkflowMetaData();
			wfmd.getMetaResult().setTableName(outTableName);
			wfmd.getMetaData().setName(outTableName);

			String sql = "";
			TaskDataSet tds = task.getDatasets().get(tdsId);

			boolean firstFilter = false;
			String whereClause = "";
			String selectClause = "";
			int fieldsCount = 0;
			List<TaskDataSetModifier> modifiers = tds.getModifiers();

			List<Field> fields = new ArrayList<Field>();
			wfmd.getMetaResult().setFields(fields);

			if (modifiers.size() > 0) {
				for (TaskDataSetModifier modifier : modifiers) {
					String exp = modifier.getExpression();
					String alias = modifier.getAlias();
					TaskDataSetProperty tdsp = modifier.getProperty();
					String tableName = tdsp.getTableName();
					if (selectClause.length() > 0) {
						selectClause += ", ";
					}
					String colTableName = tdsp.getTableName();
					selectClause += exp + "(" + config.getImpalaDbName() + "."
							+ colTableName + "." + tdsp.getUniversalName()
							+ ") " + alias;

					Field f = new Field();
					f.setName(alias);
					f.setType(tdsp.getUniversalType());
					fields.add(f);

					if (!tables.contains(colTableName)) {
						tables.add(colTableName);
					}
					fieldsCount++;
				}
			} else {
				for (TaskDataSetProperty field : tds.getFields()) {
					if (selectClause.length() > 0) {
						selectClause += ", ";
					}

					Field f = new Field();
					f.setName(field.getUniversalName());
					f.setType(field.getUniversalType());
					fields.add(f);

					selectClause += config.getImpalaDbName() + "."
							+ field.getTableName() + "."
							+ field.getUniversalName();
					String colTableName = field.getTableName();
					if (!tables.contains(colTableName)) {
						tables.add(colTableName);
					}
					fieldsCount++;
				}
			}

			wfmd.getMetaData().setFieldsCount(fieldsCount);

			for (TaskDataSetFilter filter : tds.getFilters()) {
				String filterType = filter.getType();
				TaskDataSetProperty prop = filter.getTaskDataSetProperty();
				String tableName = prop.getTableName();

				String operator = " = ";
				String exp = filter.getExpression();
				if (exp.equalsIgnoreCase("lt")) {
					operator = " < ";
				} else if (exp.equalsIgnoreCase("le")) {
					operator = " <= ";
				} else if (exp.equalsIgnoreCase("gt")) {
					operator = " > ";
				} else if (exp.equalsIgnoreCase("ge")) {
					operator = " >= ";
				} else if (exp.equalsIgnoreCase("eq")) {
					operator = " = ";
				} else if (exp.equalsIgnoreCase("ne")) {
					operator = " <> ";
				} else if (exp != null && !exp.equals("")) {
					throw new OtterException("Invalid expression: " + exp);
				}
				if (whereClause.length() > 0) {
					whereClause += " AND ";
				}
				String propName = prop.getUniversalName();
				String propType = prop.getUniversalType();
				String val = filter.getValue();
				if (propType.equalsIgnoreCase("real")
						|| propType.equalsIgnoreCase("float")
						|| propType.equalsIgnoreCase("number")
						|| propType.equalsIgnoreCase("double")) {
					String noQuotes = val.replaceAll("\"", "");
					try {
						val = String.valueOf(Integer.parseInt(noQuotes));
					} catch (NumberFormatException nfe) {
						try {
							val = String.valueOf(Double.parseDouble(noQuotes));
						} catch (NumberFormatException nfe2) {
							throw new OtterException("Cannot compare "
									+ propName + " (" + propType
									+ ") to value " + val);
						}
					}

				}
				whereClause += "(" + propName + " " + operator + " " + val
						+ " ) ";
			}

			String groupByClause = "";
			for (TaskDataSetModifierGroup group : tds.getGroups()) {
				TaskDataSetProperty prop = group.getProperty();
				String tableName = prop.getTableName();
				if (groupByClause.length() > 0) {
					groupByClause += ", ";
				}
				groupByClause += tableName + "." + prop.getUniversalName();
			}

			String orderByClause = "";
			for (TaskDataSetModifierSort sort : tds.getSorts()) {
				TaskDataSetProperty prop = sort.getProperty();
				String dir = sort.getDirection();
				String tableName = prop.getTableName();
				if (orderByClause.length() > 0) {
					orderByClause += ", ";
				}
				orderByClause += tableName + "." + prop.getUniversalName()
						+ " " + dir;
			}
			if (tables.size() > 1) {
				throw new OtterException("Joins unsupported yet.");
			}
			fromClause = config.getImpalaDbName() + "." + tables.get(0);
			sql = "SELECT " + selectClause + " FROM " + fromClause;
			if (whereClause.length() > 0) {
				sql += " WHERE " + whereClause;
			}

			if (groupByClause.length() > 0) {
				sql += " GROUP BY " + groupByClause;
			}

			if (orderByClause.length() > 0) {
				sql += " ORDER BY " + orderByClause;
			}
			sql = "CREATE TABLE " + config.getImpalaDbName() + "." + outTableName
					+ " AS " + sql;
			wfmd.setSql(sql);
			retval.add(wfmd);
		}
		return retval;
	}

	public void connect() throws SQLException {
		Config config = Config.getInstance();
		String impalaHost = config.getProperty(Config.PROP_CDH_HOST);
		String url = "jdbc:hive2://" + impalaHost + ":21050/;auth=noSasl";
		Logger.log("Connecting to " + url + "...");
		Class klass0 = org.apache.hadoop.hive.jdbc.HiveDriver.class;
		Class klass = klass0;
		try {
			klass = Class.forName("org.apache.hive.jdbc.HiveDriver");
		} catch (ClassNotFoundException cnfe) {
			//
		}
		con = DriverManager.getConnection(url);
		Logger.log("Connected to " + url + ".");
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

	public void drop(Dataset ds) throws SQLException {
		Connection c = getConnection();
		String sql = "DROP TABLE " + getTableName(ds);
		PreparedStatement ps = c.prepareStatement(sql);
		ps.execute();
	}

	// public void deleteDataset(String name) throws OtterException {
	// try {
	// Connection c = getConnection();
	// String sql = "DELETE FROM " + config.getImpalaDbName() + "." + name;
	// PreparedStatement ps = c.prepareStatement(sql);
	// ps.execute();
	// refreshTable(name);
	// } catch (SQLException sqle) {
	// throw new OtterException(sqle);
	// }
	// }

	private Connection getConnection() throws SQLException {
		if (con == null || con.isClosed()) {
			connect();
		}
		return con;
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

	@SuppressWarnings("unchecked")
	public List getSample(String tableName, int limit) throws SQLException {
		Connection c = getConnection();
		String sql = "SELECT * FROM " + config.getImpalaDbName() + "."
				+ tableName;
		if (limit > 0) {
			sql += " LIMIT " + limit;
		}

		PreparedStatement ps = c.prepareStatement(sql);
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

	private String getTableName(Dataset ds) {
		String fqtn = config.getImpalaDbName() + "." + ds.getName();
		// Cthulhu fqtn!
		return fqtn;
	}

	public Map query(String query) throws SQLException {
		return query(query, false);
	}

	public Map query(String query, boolean metadataOnly) throws SQLException {
		Map map = new HashMap<>();
		Connection c = getConnection();
		List list = new ArrayList<List>();
		PreparedStatement ps = c.prepareStatement("USE "
				+ config.getImpalaDbName());
		ps.execute();
		if (metadataOnly) {
			query = "SELECT * FROM (" + query + " ) LIMIT 0";
		}
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

	public void refreshTable(Dataset ds) throws SQLException {
		Connection c = getConnection();
		String sql = "ALTER TABLE " + getTableName(ds) + " " + "SET LOCATION '"
				+ config.getOtterHdfsPrefix() + ds.getName() + "'";
		PreparedStatement ps = c.prepareStatement(sql);
		ps.execute();
	}

	public List<String> testCleanup() {
		List<String> errors = new ArrayList<String>();
		try {

			Connection c = getConnection();

			String dbName = config.getImpalaDbName();
			PreparedStatement ps0 = c.prepareStatement("USE " + dbName);
			ps0.execute();

			String sql = "SHOW TABLES";
			PreparedStatement ps = c.prepareStatement(sql);
			ResultSet rs = ps.executeQuery();
			ResultSetMetaData rsmd = rs.getMetaData();
			// for (int i = 1; i <= rsmd.getColumnCount(); i++) {
			// // System.out.println(rsmd.getColumnName(i));
			// }
			while (rs.next()) {
				try {
					String tableName = rs.getString("name");
					if (tableName.toLowerCase().startsWith("test")) {
						String sqlDrop = "DROP TABLE "
								+ config.getImpalaDbName() + "." + tableName;
						Logger.log(sqlDrop);
						PreparedStatement ps2 = c.prepareStatement(sqlDrop);
						ps2.execute();
						ps2.close();
					}
				} catch (SQLException sqle) {
					if (sqle.getMessage().equalsIgnoreCase(
							"AnalysisException: Table does not exist: ")) {
						// Ignore
					} else {
						errors.add(sqle.getMessage());
					}
				}
			}
			ps.close();
			rs.close();
			;
		} catch (SQLException sqle2) {
			errors.add(sqle2.getMessage());
		}
		if (errors.size() == 0) {
			return null;
		}
		return errors;
	}

	public void updateDataset(Dataset ds) throws SQLException,
			ClassNotFoundException {
		Connection c = getConnection();
		// Dropping external tables doesn't change anything...
		PreparedStatement ps = c.prepareStatement("DROP TABLE "
				+ getTableName(ds));
		ps.execute();
		addDataset(ds);
		refreshTable(ds);
	}
}
