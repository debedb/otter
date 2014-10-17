package com.enremmeta.otter;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.enremmeta.otter.entity.Dataset;
import com.enremmeta.otter.entity.messages.DatasetError;
import com.enremmeta.otter.entity.messages.DatasetLoadMessage;
import com.enremmeta.otter.entity.messages.DatasetLoadSource;
import com.enremmeta.otter.entity.messages.DatasetSuccess;
import com.enremmeta.otter.entity.messages.EmptyMessage;
import com.enremmeta.otter.entity.messages.Field;
import com.enremmeta.otter.entity.messages.IdMessage;
import com.enremmeta.otter.entity.messages.MetaResult;
import com.enremmeta.otter.entity.messages.OtterMessage;
import com.enremmeta.otter.entity.messages.QueryMessage;
import com.enremmeta.otter.entity.messages.TableMetaData;
import com.enremmeta.otter.entity.messages.TaskInfoResultSaved;
import com.enremmeta.otter.entity.messages.TaskExecutionStatusNotification;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Message;

/**
 * What's another animal, Otter, Rabbit. The purpose of this
 * 
 * 
 * @author
 */
public class Workhorse {
	private OfficeDb odb = new OfficeDb();
	private Impala imp = new Impala();
	private CdhConnection cdhc = new CdhConnection();
	private AsyncStatusHandler asyncStatusHandler;

	public Workhorse(AsyncStatusHandler asyncStatusHandler) {
		super();
		this.asyncStatusHandler = asyncStatusHandler;
	}

	private ObjectMapper mapper = new ObjectMapper();

	public void connect() throws OtterException {
		try {
			odb.connect();
			cdhc.connect();
			imp.connect();
		} catch (Exception e) {
			throw new OtterException(e);
		}
	}

	public Map query(QueryMessage msg) throws Exception {
		String query = msg.getQuery();
		Map map = imp.query(query);
		return map;
	}

	public OtterMessage dispatch(String op, String payload) throws Throwable {
		if (payload != null) {
			payload = payload.trim();
			if (payload.length() == 0) {
				payload = null;
			}
		}
		String methodName = Utils.camelCase(op);
		Method methods[] = getClass().getMethods();
		for (Method m : methods) {
			if (!Modifier.isPublic(m.getModifiers())) {
				continue;
			}
			if (m.getName().equals(methodName)) {
				Class<?> paramTypes[] = m.getParameterTypes();
				int paramCount = paramTypes.length;
				if (paramCount > 1) {
					Logger.log("Considered " + methodName + " for " + op
							+ ": wrong param count: " + paramCount);
					continue;
				}
				Class<?> retType = m.getReturnType();
				if (!OtterMessage.class.isAssignableFrom(retType)) {
					Logger.log("Considered " + methodName + " for " + op
							+ ": wrong return type: " + m.getReturnType());
					continue;
				}

				Object val = null;
				if (payload != null) {
					try {
						val = mapper.readValue(payload, paramTypes[0]);
					} catch (IOException jme) {
						Logger.log("Error parsing " + payload + " with "
								+ paramTypes[0].getName() + ": " + jme);
						continue;
					}
				}
				OtterMessage retval;
				try {
					if (paramCount == 1) {
						retval = (OtterMessage) m.invoke(this, val);
					} else {
						retval = (OtterMessage) m.invoke(this);
					}
					return retval;
				} catch (IllegalAccessException | IllegalArgumentException e) {
					throw new OtterException(e);
				} catch (InvocationTargetException e2) {
					throw new TargetException(e2.getTargetException());
				}
			}
		}
		throw new BadRequestException("Uknown operation: " + op);
	}

	public OtterMessage datasetDelete(DatasetLoadMessage msg) throws Exception {
		Dataset ds;
		long id = msg.getId();
		ds = odb.getDataset(id);
		String dsName = ds.getName();
		cdhc.deleteDataset(dsName);
		imp.refreshTable(ds);
		IdMessage retval = new IdMessage();
		retval.setId(id);
		return retval;
	}

	public OtterMessage datasetLoad(DatasetLoadMessage msg) throws Exception {
		long id = msg.getId();
		Map map = new HashMap();
		Config config = Config.getInstance();

		String mode = msg.getMode();
		if (!mode.equalsIgnoreCase("rewrite")
				&& !mode.equalsIgnoreCase("append")) {
			throw new BadRequestException("Invalid mode " + mode
					+ " for datasetLoad operation");
		}
		if (mode.equalsIgnoreCase("rewrite")) {
			datasetDelete(msg);
		}

		Dataset ds = odb.getDataset(id);
		String dsName = ds.getName();
		long rowsBefore = imp.getCount(Config.getInstance().getImpalaDbName()
				+ "." + dsName);
		for (DatasetLoadSource source : msg.getSources()) {
			long location = source.getLocation();
			String delim = ",";
			String sourceType = config.getProperty("source." + location
					+ ".type");
			if (!sourceType.equalsIgnoreCase("s3")) {
				String errorMessage = "Unsupported source type: " + sourceType
						+ " (location: " + location + ")";
				OtterException e = new OtterException(errorMessage);
				throw e;
			}
			String s3Bucket = config.getProperty("source." + location
					+ ".bucket");
			String accessKey = config.getProperty("source." + location
					+ ".access");
			String secretKey = config.getProperty("source." + location
					+ ".secret");
			String path = source.getPath();
			cdhc.loadDataFromS3(s3Bucket, path, accessKey, secretKey, dsName);
		}
		imp.refreshTable(ds);
		long rowsAfter = imp.getCount(Config.getInstance().getImpalaDbName()
				+ "." + dsName);
		map.put("rows_before", rowsBefore);
		map.put("rows_after", rowsAfter);
		IdMessage retval = new IdMessage();
		retval.setId(id);
		return retval;
	}

	public OtterMessage datasetCreate(IdMessage msg) throws Exception {
		long id = msg.getId();
		Dataset ds = odb.getDataset(id);
		cdhc.addDataset(ds);
		imp.addDataset(ds);
		return msg;
	}

	public OtterMessage datasetDrop(IdMessage msg) throws Exception {
		long id = msg.getId();
		Dataset ds = odb.getDataset(id);
		imp.drop(ds);
		cdhc.drop(ds.getName());
		return msg;
	}

	public OtterMessage testCleanup() throws Exception {
		List<String> errs = imp.testCleanup();
		String err2 = cdhc.testCleanup();
		if (err2 != null) {
			if (errs == null) {
				errs = new ArrayList<String>();
			}
			errs.add(err2);
		}
		return new EmptyMessage();
	}

	public OtterMessage taskRun(IdMessage msg) throws Exception {
		long workflowId = 23;
		long id = msg.getId();
		TaskExecutionStatusNotification taskStatus = new TaskExecutionStatusNotification();
		taskStatus.setTaskId(id);
		taskStatus.setStatus("started");
		taskStatus.setWorkflowId(workflowId);
		taskStatus.setTimestamp(System.currentTimeMillis());
		asyncStatusHandler.handle(taskStatus);

		taskStatus = new TaskExecutionStatusNotification();
		taskStatus.setTaskId(id);
		taskStatus.setStatus("result_saving");
		taskStatus.setWorkflowId(workflowId);
		taskStatus.setTimestamp(System.currentTimeMillis());

		TaskInfoResultSaved resultSaving = new TaskInfoResultSaved();
		taskStatus.setInfo(resultSaving);

		List<TableMetaData> resultTables = new ArrayList<TableMetaData>();
		resultSaving.setResultTables(resultTables);

		TableMetaData resultTable = new TableMetaData();
		resultTables.add(resultTable);

		resultTable.setFieldsCount(37);
		resultTable.setName("customers");
		resultTable.setRowsCount(1100000000);
		resultTable.setSize(3241241231l);

		asyncStatusHandler.handle(taskStatus);

		// TODO fake
		taskStatus.setStatus("result_saved");

		MetaResult metaResult = new MetaResult();
		metaResult.setTableName("result1");
		Field f1 = new Field();
		f1.setName("name");
		f1.setType("string");
		metaResult.getFields().add(f1);
		Field f2 = new Field();
		f2.setName("value");
		f2.setType("string");
		metaResult.getFields().add(f2);
		taskStatus.setMetaResult(metaResult);
		return taskStatus;
	}

	public OtterMessage datasetUpdate(IdMessage msg) throws Exception {
		long id = msg.getId();
		Dataset ds = odb.getDataset(id);
		imp.updateDataset(ds);
		return msg;
	}

}
