package com.enremmeta.otter;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.enremmeta.otter.entity.Algorithm;
import com.enremmeta.otter.entity.Dataset;
import com.enremmeta.otter.entity.Task;
import com.enremmeta.otter.entity.TaskDataSet;
import com.enremmeta.otter.entity.messages.DatasetLoadMessage;
import com.enremmeta.otter.entity.messages.DatasetLoadSource;
import com.enremmeta.otter.entity.messages.EmptyMessage;
import com.enremmeta.otter.entity.messages.IdMessage;
import com.enremmeta.otter.entity.messages.MetaData;
import com.enremmeta.otter.entity.messages.MetaResult;
import com.enremmeta.otter.entity.messages.OtterMessage;
import com.enremmeta.otter.entity.messages.QueryMessage;
import com.enremmeta.otter.entity.messages.StatusMessage;
import com.enremmeta.otter.entity.messages.TaskExecutionStatusNotification;
import com.enremmeta.otter.entity.messages.TaskInfoError;
import com.enremmeta.otter.entity.messages.TaskInfoResultSaved;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

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

    public static final ObjectMapper MAPPER = new ObjectMapper();
    static {
	MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

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

    public OtterMessage dispatch(String op, String payload)
	    throws BadRequestException, InvocationTargetException {

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
		    if (paramTypes.length == 0) {
			continue;
		    }
		    try {
			val = MAPPER.readValue(payload, paramTypes[0]);
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
		    throw new BadRequestException(e);
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
	Logger.log("In datasetLoad(): Rows before: " + rowsBefore
		+ "; rows after: " + rowsAfter);
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
	StatusMessage sm = new StatusMessage();
	try {
	    List<String> errs = imp.testCleanup();
	    String err2 = cdhc.testCleanup();
	    if (err2 != null) {
		if (errs == null) {
		    errs = new ArrayList<String>();
		}
		errs.add(err2);
	    }
	    sm.setStatus(true);
	} catch (Exception e) {
	    sm.setStatus(false);
	    sm.setError(e.getMessage());
	}
	return sm;
    }

    public OtterMessage noop() {
	return new EmptyMessage();
    }

    private long workflowId = 1;

    public OtterMessage taskRun(IdMessage msg) throws Exception {
	Logger.log("ENTERING taskRun() - code version " + Constants.VERSION);
	workflowId++;
	long id = msg.getId();

	// Step 1. Send that we started
	TaskExecutionStatusNotification taskStatus = new TaskExecutionStatusNotification();
	taskStatus.setTaskId(id);
	taskStatus.setStatus("started");
	taskStatus.setWorkflowId(workflowId);
	taskStatus.setTimestamp(System.currentTimeMillis());
	asyncStatusHandler.handle(taskStatus);

	Task mainTask = null;

	try {
	    mainTask = odb.getTask(id);
	} catch (OtterException e) {
	    TaskExecutionStatusNotification errStatus = new TaskExecutionStatusNotification();
	    errStatus.setTaskId(id);
	    errStatus.setStatus("error");
	    errStatus.setWorkflowId(workflowId);
	    errStatus.setTimestamp(System.currentTimeMillis());
	    TaskInfoError errInfo = new TaskInfoError();
	    errInfo.setError(e.getMessage());
	    errStatus.setInfo(errInfo);
	    return errStatus;
	}
	Logger.log("Task to run: " + mainTask);

	List<Task> tasksToRun;
	if (mainTask.isComplex()) {
	    tasksToRun = mainTask.getSubtasks();
	    Logger.log("Complex task, will run subtasks:" + tasksToRun);
	} else {
	    tasksToRun = new ArrayList<Task>(1);
	    tasksToRun.add(mainTask);
	}

	for (Task task : tasksToRun) {
	    Algorithm alg = task.getAlgorithm();

	    if (alg == null) {
		TaskExecutionStatusNotification errStatus = new TaskExecutionStatusNotification();
		errStatus.setTaskId(id);
		errStatus.setStatus("error");
		errStatus.setWorkflowId(workflowId);
		errStatus.setTimestamp(System.currentTimeMillis());
		TaskInfoError errInfo = new TaskInfoError();
		errInfo.setError("No algorithm found.");
		errStatus.setInfo(errInfo);

		asyncStatusHandler.handle(errStatus);
		continue;
	    }
	    if (!"copy".equalsIgnoreCase(alg.getName())) {
		TaskExecutionStatusNotification errStatus = new TaskExecutionStatusNotification();
		errStatus.setTaskId(id);
		errStatus.setStatus("error");
		errStatus.setWorkflowId(workflowId);
		errStatus.setTimestamp(System.currentTimeMillis());
		TaskInfoError errInfo = new TaskInfoError();
		errInfo.setError("Algorithm not yet supported: "
			+ alg.getName());
		errStatus.setInfo(errInfo);
		asyncStatusHandler.handle(errStatus);
		continue;
	    }

	    Map<Long, TaskDataSet> datasets = task.getDatasets();
	    List<WorkflowMetaData> wfs = imp.buildWorkflow(task, String
		    .valueOf(workflowId));

	    List<MetaData> resultTables = new ArrayList<MetaData>();

	    // Prepare
	    for (WorkflowMetaData wf : wfs) {
		resultTables.add(wf.getMetaData());
	    }

	    TaskExecutionStatusNotification savingStatus = new TaskExecutionStatusNotification();
	    savingStatus.setTaskId(id);
	    savingStatus.setStatus("result_saving");
	    savingStatus.setWorkflowId(workflowId);
	    savingStatus.setTimestamp(System.currentTimeMillis());
	    TaskInfoResultSaved infoSaving = new TaskInfoResultSaved();
	    savingStatus.setInfo(infoSaving);
	    infoSaving.setResultTables(resultTables);
	    asyncStatusHandler.handle(savingStatus);

	    // Now, run..
	    TaskExecutionStatusNotification savedStatus = new TaskExecutionStatusNotification();
	    savedStatus.setTaskId(id);
	    savedStatus.setStatus("result_saved");
	    savedStatus.setWorkflowId(workflowId);
	    savedStatus.setTimestamp(System.currentTimeMillis());
	    TaskInfoResultSaved infoSaved = new TaskInfoResultSaved();
	    savedStatus.setInfo(infoSaved);

	    List<MetaResult> metaResult = new ArrayList<MetaResult>();
	    savedStatus.setMetaResult(metaResult);

	    List<MetaData> resultTables2 = new ArrayList<MetaData>();
	    infoSaved.setResultTables(resultTables2);

	    boolean anyWfSucceeded = false;
	    for (WorkflowMetaData wf : wfs) {
		try {
		    long cnt = imp.runWorkflow(wf);
		    wf.getMetaData().setRowsCount(cnt);
		    resultTables2.add(wf.getMetaData());
		    metaResult.add(wf.getMetaResult());
		    anyWfSucceeded = true;
		} catch (Exception e) {
		    TaskExecutionStatusNotification errStatus = new TaskExecutionStatusNotification();
		    errStatus.setTaskId(id);
		    errStatus.setStatus("error");
		    errStatus.setWorkflowId(workflowId);
		    errStatus.setTimestamp(System.currentTimeMillis());
		    TaskInfoError errInfo = new TaskInfoError();
		    errInfo.setError(e.getMessage());
		    errStatus.setInfo(errInfo);
		    asyncStatusHandler.handle(errStatus);
		}
	    }
	    if (anyWfSucceeded) {
		asyncStatusHandler.handle(savedStatus);
	    }
	}
	return null;
    }

    public OtterMessage datasetUpdate(IdMessage msg) throws Exception {
	long id = msg.getId();
	Dataset ds = odb.getDataset(id);
	imp.updateDataset(ds);
	return msg;
    }

}
