package com.enremmeta.otter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.enremmeta.otter.entity.Algorithm;
import com.enremmeta.otter.entity.Dataset;
import com.enremmeta.otter.entity.DatasetColumn;
import com.enremmeta.otter.entity.Task;
import com.enremmeta.otter.entity.TaskDataSet;
import com.enremmeta.otter.entity.TaskDataSetFilter;
import com.enremmeta.otter.entity.TaskDataSetModifier;
import com.enremmeta.otter.entity.TaskDataSetModifierGroup;
import com.enremmeta.otter.entity.TaskDataSetModifierSort;
import com.enremmeta.otter.entity.TaskDataSetProperty;

public class OfficeDb {

	public OfficeDb() {
		super();
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

	private static final String TASK_FILTERS_SQL = "SELECT t.name TaskName, "
			+ "tds.id TdsId,"
			+ "tds.name TaskDataSetName, "
			+ "tdsf.id TdsfId, "
			+ "tdsf.connect_operator FilterOp, "
			+ "tdsf.type FilterType, "
			+ "tdsf.expression FilterExp, "
			+ "tdsf.value FilterValue, "
			+ "tdsf.parameter_id FilterParamId, "
			+ "tdsp.id TdspId, "
			+ "tdsp.alias PropAlias, "
			+ " up.title UniversalPropertyTitle, "
			+ " up.name UniversalPropertyName, "
			+ " up.type UniversalPropertyType, "
			+ " up.type_enum_values UniversalPropertyEnumVals, "
			+ " us.name DbName "
			+ "FROM task t "
			+ "JOIN task_data_set tds ON t.id = tds.task_id "
			+ "JOIN task_data_set_filter tdsf ON tdsf.task_data_set_id = tds.id "
			+ "JOIN task_data_set_property tdsp  "
			+ "ON tdsf.task_data_set_property_id = tdsp.id "
			+ "JOIN task_data_set_source tdss ON tdsp.task_data_set_source_id = tdss.id "
			+ "JOIN universal_source us ON tdss.universal_source_id = us.id "
			+ "JOIN task_data_set tds2 ON tdsp.task_data_set_id = tds2.id "
			+ "JOIN universal_property up ON tdsp.universal_property_id = up.id "
			+ "WHERE  t.id = ? ORDER BY tds.id, tdsf.id, tdsp.id, up.id, us.id";

	private static final String TASK_MODIFIERS_SQL = "SELECT tds.id TdsId, tdsm.id TdsmId, "
			+ "tds.name TaskDataSetName, "
			+ "tdsm.alias ModifierAlias, "
			+ " tdsm.expression ModifierExpression, tdsp.id TdspId, tdsp.alias PropAlias, up.title UniversalPropertyTitle, "
			+ " up.name UniversalPropertyName, up.type UniversalPropertyType, us.name DbName "
			+ " FROM task_data_set tds "
			+ " JOIN task_data_set_modifier tdsm ON tdsm.task_data_set_id = tds.id "
			+ " JOIN task_data_set_property tdsp "
			+ " ON tdsm.task_data_set_property_id = tdsp.id "
			+ " JOIN task_data_set_source tdss ON tdsp.task_data_set_source_id = tdss.id "
			+ " JOIN universal_source us ON tdss.universal_source_id = us.id "
			+ " JOIN universal_property up ON tdsp.universal_property_id = up.id "
			+ " WHERE "
			+ " tds.task_id = ? ORDER BY tds.id, tdsm.id, up.id, us.id";

	private static final String TASK_MODIFIERS_SORT_SQL = "SELECT tds.id TdsId, "
			+ "tds.name TaskDataSetName, "
			+ " tdsms.id TdsmsId, "
			+ " tdsms.direction Direction, tdsp.id TdspId, "
			+ " tdsp.alias PropAlias, up.title UniversalPropertyTitle, "
			+ " up.name UniversalPropertyName, up.type UniversalPropertyType, us.name DbName "
			+ " FROM task_data_set tds "
			+ " JOIN task_data_set_modifier_sort tdsms ON tdsms.task_data_set_id = tds.id "
			+ " JOIN task_data_set_property tdsp "
			+ " ON tdsms.task_data_set_property_id = tdsp.id "
			+ " JOIN task_data_set_source tdss ON tdsp.task_data_set_source_id = tdss.id "
			+ " JOIN universal_source us ON tdss.universal_source_id = us.id "
			+ " JOIN universal_property up ON tdsp.universal_property_id = up.id "
			+ " WHERE "
			+ " tds.task_id = ? ORDER BY tds.id, tdsms.id, up.id, us.id";

	private static final String TASK_MODIFIERS_GROUP_SQL = "SELECT tds.id TdsId, tds.name TaskDataSetName, tdsmg.id TdsmgId, "
			+ "tdsp.id TdspId, "
			+ " tdsp.alias PropAlias, up.title UniversalPropertyTitle, "
			+ " up.name UniversalPropertyName, up.type UniversalPropertyType, us.name DbName "
			+ " FROM task_data_set tds "
			+ " JOIN task_data_set_modifier_group tdsmg ON tdsmg.task_data_set_id = tds.id "
			+ " JOIN task_data_set_property tdsp "
			+ " ON tdsmg.task_data_set_property_id = tdsp.id "
			+ " JOIN task_data_set_source tdss ON tdsp.task_data_set_source_id = tdss.id "
			+ " JOIN universal_source us ON tdss.universal_source_id = us.id "
			+ " JOIN universal_property up ON tdsp.universal_property_id = up.id "
			+ " WHERE "
			+ " tds.task_id = ? ORDER BY tds.id, tdsmg.id, up.id, us.id";

	private static final String TASK_ALGORITHM_SQL = "SELECT a.id AlgId, a.name AlgName,  a.process AlgProcess "
			+ " FROM algorithm a "
			+ " JOIN task_algorithm ta ON a.id = ta.algorithm_id"
			+ " JOIN task t ON t.id = ta.task_id " + " WHERE t.id = ?";

	private static final String TASK_DATASET_SQL = "SELECT  t.name TaskName, tds.id TdsId, tds.name TaskDataSetName, "
			+ " tdsp.id TdspId, tdsp.alias PropAlias, up.title UniversalPropertyTitle,  "
			+ " up.name UniversalPropertyName,  up.type UniversalPropertyType,  "
			+ " up.type_enum_values UniversalPropertyEnumVals, us.name DbName "
			+ " FROM task t JOIN task_data_set tds ON t.id = tds.task_id "
			+ " JOIN task_data_set_property tdsp  ON tds.id = tdsp.task_data_set_id "
			+ " JOIN task_data_set_source tdss ON tdsp.task_data_set_source_id = tdss.id "
			+ " JOIN universal_source us ON tdss.universal_source_id = us.id "
			+ " JOIN universal_property up ON tdsp.universal_property_id = up.id "
			+ " WHERE  t.id = ? " + " ORDER BY tds.id, tdsp.id, up.id, us.id";

	private void loadTaskModifiers(Task t) throws SQLException {
		Connection c = getConnection();
		PreparedStatement ps = c.prepareStatement(TASK_MODIFIERS_SQL);
		ps.setLong(1, t.getId());
		ResultSet rs = ps.executeQuery();

		long prevTdsId = -1;
		long prevTdsmId = -1;
		long prevTdspId = -1;

		TaskDataSet tds = null;
		TaskDataSetModifier tdsm = null;

		while (rs.next()) {
			int i = 1;

			long tdsId = rs.getLong("TdsId");

			if (tdsId != prevTdsId) {
				tds = t.getDatasets().get(tdsId);
				if (tds == null) {
					throw new RuntimeException("Dataset " + tdsId
							+ " not found!");
				}
				String tdsName = rs.getString("TaskDataSetName");
				tds.setName(tdsName);
				prevTdsId = tdsId;
			}

			long tdsmId = rs.getLong("TdsmId");
			if (tdsmId != prevTdsmId) {
				tdsm = new TaskDataSetModifier(tdsmId);
				tdsm.setAlias(rs.getString("ModifierAlias"));
				tdsm.setExpression(rs.getString("ModifierExpression"));

				tds.getModifiers().add(tdsm);

				TaskDataSetProperty tdsp = loadProperty(rs);
				tdsm.setProperty(tdsp);
				// Ignore DB Name for now
				prevTdsmId = tdsmId;
			}
		}
	}

	private void loadTaskAlgorithm(Task t) throws SQLException {
		Connection c = getConnection();
		PreparedStatement ps = c.prepareStatement(TASK_ALGORITHM_SQL);
		ps.setLong(1, t.getId());
		ResultSet rs = ps.executeQuery();
		// TODO many to many
		if (rs.next()) {
			Algorithm alg = new Algorithm();
			alg.setId(rs.getLong("AlgId"));
			alg.setName(rs.getString("AlgName"));

			alg.setProcess(rs.getString("AlgProcess"));
			t.setAlgorithm(alg);
		}

	}

	private void loadTaskDataSets(Task t) throws SQLException {
		Connection c = getConnection();
		PreparedStatement ps = c.prepareStatement(TASK_DATASET_SQL);
		ps.setLong(1, t.getId());
		ResultSet rs = ps.executeQuery();

		long prevTdsId = -1;

		TaskDataSet tds = null;
		TaskDataSetModifierSort tdsms = null;

		while (rs.next()) {
			int i = 1;
			long tdsId = rs.getLong("TdsId");

			if (tdsId != prevTdsId) {
				// New dataset? Or already existed?
				tds = new TaskDataSet(tdsId);
				String tdsName = rs.getString("TaskDataSetName");
				tds.setName(tdsName);
				t.getDatasets().put(tdsId, tds);
				prevTdsId = tdsId;
			}
			long tdspId = rs.getLong("TdspId");
			TaskDataSetProperty tdsp = loadProperty(rs);
			tds.getFields().add(tdsp);
		}
	}

	private void loadTaskModifiersSort(Task t) throws SQLException {
		Connection c = getConnection();
		PreparedStatement ps = c.prepareStatement(TASK_MODIFIERS_SORT_SQL);
		ps.setLong(1, t.getId());
		ResultSet rs = ps.executeQuery();

		long prevTdsId = -1;
		long prevTdsmsId = -1;
		long prevTdspId = -1;

		TaskDataSet tds = null;
		TaskDataSetModifierSort tdsms = null;

		while (rs.next()) {
			int i = 1;
			long tdsId = rs.getLong("TdsId");

			if (tdsId != prevTdsId) {
				tds = t.getDatasets().get(tdsId);
				if (tds == null) {
					throw new RuntimeException("Dataset " + tdsId
							+ " not found!");
				}
				String tdsName = rs.getString("TaskDataSetName");
				tds.setName(tdsName);
				prevTdsId = tdsId;
			}

			long tdsmsId = rs.getLong("TdsmsId");
			if (tdsmsId != prevTdsmsId) {
				tdsms = new TaskDataSetModifierSort(tdsmsId);
				tdsms.setDirection(rs.getString("Direction"));

				tds.getSorts().add(tdsms);

				TaskDataSetProperty tdsp = loadProperty(rs);
				tdsms.setProperty(tdsp);
				// Ignore DB Name for now
				prevTdsmsId = tdsmsId;
			}
		}
	}

	private void loadTaskModifiersGroup(Task t) throws SQLException {
		Connection c = getConnection();
		PreparedStatement ps = c.prepareStatement(TASK_MODIFIERS_GROUP_SQL);
		ps.setLong(1, t.getId());
		ResultSet rs = ps.executeQuery();

		long prevTdsId = -1;
		long prevTdsmgId = -1;
		long prevTdspId = -1;

		TaskDataSet tds = null;
		TaskDataSetModifierGroup tdsmg = null;

		while (rs.next()) {
			int i = 1;

			long tdsId = rs.getLong("TdsId");

			if (tdsId != prevTdsId) {
				tds = t.getDatasets().get(tdsId);
				if (tds == null) {
					throw new RuntimeException("Dataset " + tdsId
							+ " not found!");
				}
				String tdsName = rs.getString("TaskDataSetName");
				tds.setName(tdsName);
				prevTdsId = tdsId;
			}

			long tdsmgId = rs.getLong("TdsmgId");
			if (tdsmgId != prevTdsmgId) {
				tdsmg = new TaskDataSetModifierGroup(tdsmgId);

				tds.getGroups().add(tdsmg);

				TaskDataSetProperty tdsp = loadProperty(rs);
				tdsmg.setProperty(tdsp);
				// Ignore DB Name for now
				prevTdsmgId = tdsmgId;
			}
		}
	}

	private TaskDataSetProperty loadProperty(ResultSet rs) throws SQLException {
		TaskDataSetProperty tdsp = new TaskDataSetProperty(rs.getLong("TdspId"));
		tdsp.setAlias(rs.getString("PropAlias"));
		tdsp.setUniversalName(rs.getString("UniversalPropertyName"));
		tdsp.setUniversalTitle(rs.getString("UniversalPropertyTitle"));
		tdsp.setUniversalType(rs.getString("UniversalPropertyType"));
		tdsp.setTableName(rs.getString("DbName"));
		return tdsp;
	}

	private void loadTaskFilters(Task t) throws SQLException {
		Connection c = getConnection();
		PreparedStatement ps = c.prepareStatement(TASK_FILTERS_SQL);
		ps.setLong(1, t.getId());
		ResultSet rs = ps.executeQuery();

		long prevTdsId = -1;
		long prevTdsfId = -1;
		long prevTdspId = -1;

		TaskDataSetFilter tdsf = null;

		TaskDataSet tds = null;
		while (rs.next()) {
			int i = 1;

			String taskName = rs.getString("TaskName");
			t.setName(taskName);

			long tdsId = rs.getLong("TdsId");

			if (tdsId != prevTdsId) {
				tds = t.getDatasets().get(tdsId);
				if (tds == null) {
					throw new RuntimeException("Dataset " + tdsId
							+ " not found!");
				}
				String tdsName = rs.getString("TaskDataSetName");
				tds.setName(tdsName);
				prevTdsId = tdsId;
			}

			long tdsfId = rs.getLong("TdsfId");
			if (tdsfId != prevTdsfId) {
				tdsf = new TaskDataSetFilter(tdsfId);
				tdsf.setConnectOperator(rs.getString("FilterOp"));
				tdsf.setType(rs.getString("FilterType"));
				tdsf.setExpression(rs.getString("FilterExp"));
				tdsf.setValue(rs.getString("FilterValue"));
				tdsf.setParameterId(rs.getInt("FilterParamId"));
				tds.getFilters().add(tdsf);

				TaskDataSetProperty tdsp = loadProperty(rs);

				tdsf.setTaskDataSetProperty(tdsp);

				prevTdsfId = tdsfId;
			}
		}
	}

	public Task getTask(long id) throws OtterException {
		Task t = new Task();
		t.setId(id);
		try {
			loadTaskAlgorithm(t);
			loadTaskDataSets(t);
			loadTaskFilters(t);
			loadTaskModifiers(t);
			loadTaskModifiersGroup(t);
			loadTaskModifiersSort(t);

			return t;
		} catch (SQLException sqle) {
			throw new OtterException(sqle);
		}

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
		// Connection c;
		// try {
		// c = getConnection();
		// } catch (SQLException e) {
		// throw new OtterException(e);
		// }
		// try {
		// PreparedStatement ps = c
		// .prepareStatement("DELETE FROM dataset WHERE title = 'test1'");
		// int upd = ps.executeUpdate();
		// ps.close();
		// c.commit();
		// return upd;
		// } catch (SQLException sqle) {
		// rollback();
		// throw new OtterException(sqle);
		// } finally {
		// }
		return 0;
	}

	private void rollback() {
		try {
			Connection c = getConnection();
			c.rollback();
		} catch (SQLException sqle) {

		}
	}

	public Dataset getDataset(long datasetId) throws SQLException {
		Connection c = getConnection();
		PreparedStatement ps = c
				.prepareStatement("SELECT us.name, up.name, up.type FROM universal_source us "
						+ "JOIN universal_property up ON up.universal_source_id = us.id "
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
			// col.setFmt(rs.getString(4));
			retval.getColumns().add(col);
		}
		return retval;
	}
}
