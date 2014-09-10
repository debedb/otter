package com.enremmeta.otter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.enremmeta.otter.entity.Dataset;
import com.enremmeta.otter.entity.messages.BadMessageError;
import com.enremmeta.otter.entity.messages.DatasetError;
import com.enremmeta.otter.entity.messages.DatasetLoadMessage;
import com.enremmeta.otter.entity.messages.DatasetLoadSource;
import com.enremmeta.otter.entity.messages.DatasetSuccess;
import com.enremmeta.otter.entity.messages.IdMessage;
import com.enremmeta.otter.entity.messages.OtterMessage;
import com.enremmeta.otter.entity.messages.TableMetaData;
import com.enremmeta.otter.entity.messages.TaskInfoResultSaved;
import com.enremmeta.otter.entity.messages.TaskStatusMessage;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class RabbitBackendConsumer extends DefaultConsumer {

	private Rabbit rabbit;

	// private ThreadLocal<OfficeDb> odb = new ThreadLocal<OfficeDb>();
	//
	// private ThreadLocal<Impala> imp = new ThreadLocal<Impala>();
	//
	// private ThreadLocal<CdhConnection> cdhc = new
	// ThreadLocal<CdhConnection>();

	private OfficeDb odb = new OfficeDb();
	private Impala imp = new Impala();
	private CdhConnection cdhc = new CdhConnection();

	public void connect() throws OtterException {
		try {
			odb.connect();
			cdhc.connect();
			imp.connect();
		} catch (Exception e) {
			throw new OtterException(e);
		}
		rabbit.connect();
	}

	public RabbitBackendConsumer(Rabbit rabbit) {
		super(rabbit.getChannel());
		// odb.set(new OfficeDb());
		// imp.set(new Impala());
		// cdhc.set(new CdhConnection());
		this.rabbit = rabbit;
		JsonFactory jsonFactory = new JsonFactory();
		jsonFactory.configure(Feature.ALLOW_SINGLE_QUOTES, true);
		mapper = new ObjectMapper(jsonFactory);
	}

	// private String consumerTag ;

	@Override
	public void handleConsumeOk(String consumerTag) {
		// TODO Auto-generated method stub
		super.handleConsumeOk(consumerTag);
		// this.consumerTag = consumerTag;
	}

	private ObjectMapper mapper;

	private void cleanupTest() throws Exception {
		List<String> errs = imp.testCleanup();
		String err2 = cdhc.testCleanup();
		if (err2 != null) {
			if (errs == null) {
				errs = new ArrayList<String>();
			}
			errs.add(err2);
		}
	}

	private Map<String, Class> opToArgs = new HashMap<String, Class>() {
		{
			put("fb.dataset_create", IdMessage.class);
			put("dataset_create", IdMessage.class);
			put("fb.dataset_update", IdMessage.class);
			put("dataset_update", IdMessage.class);
			put("fb.dataset_load", DatasetLoadMessage.class);
			put("dataset_load", DatasetLoadMessage.class);
			put("fb.task_run", IdMessage.class);
			put("task_run", IdMessage.class);
			put("fb.test_cleanup", OtterMessage.class);
			put("test_cleanup", OtterMessage.class);

		}
	};

	private void deleteDataset(DatasetLoadMessage msg) throws Exception {
		Dataset ds;
		long id = msg.getId();
		ds = odb.getDataset(id);
		String dsName = ds.getName();
		cdhc.deleteDataset(dsName);
		imp.refreshTable(ds);
	}

	private void loadDataset(DatasetLoadMessage msg) throws Exception {
		long id = msg.getId();
		Map map = new HashMap();
		Config config = Config.getInstance();

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
	}

	private void doOp(String op, OtterMessage msg, long delTag)
			throws IOException {
		long id = -1;
		if (msg instanceof IdMessage) {
			id = ((IdMessage) msg).getId();
		}

		if (op.startsWith("dataset_")) {
			try {
				if (op.equals("dataset_create")) {
					Dataset ds = odb.getDataset(id);
					cdhc.addDataset(ds);
					imp.addDataset(ds);
				} else if (op.equals("dataset_update")) {
					Dataset ds = odb.getDataset(id);
					imp.updateDataset(ds);
				} else if (op.equals("dataset_load")) {
					DatasetLoadMessage loadMsg = (DatasetLoadMessage) msg;
					id = loadMsg.getId();
					String mode = loadMsg.getMode();
					if (mode.equalsIgnoreCase("append")) {
						loadDataset(loadMsg);
					} else if (mode.equalsIgnoreCase("rewrite")) {
						deleteDataset(loadMsg);
						loadDataset(loadMsg);
					} else {
						throw new RuntimeException("Unexpected mode " + mode);
					}
				} else {
					throw new RuntimeException("Unexpected command " + op);
				}
				try {
					DatasetSuccess succ = new DatasetSuccess();
					succ.setId(id);
					succ.setCommand(op);
					succ.setDeliveryTag(delTag);
					rabbit.send("bf.dataset_success", succ);
				} catch (IOException ioe) {
					ioe.printStackTrace();
				}
			} catch (Exception e) {
				DatasetError err = new DatasetError();
				err.setDeliveryTag(delTag);
				err.setId(id);
				err.setCommand(op);
				err.setReason(e.getMessage());
				rabbit.send("bf.dataset_error", err);
				return;
			}
		} else if (op.startsWith("task_")) {
			// TODO =- MOCK
			if (op.equals("task_run")) {
				long workflowId = 23;
				TaskStatusMessage taskStatus = new TaskStatusMessage();
				taskStatus.setTaskId(id);
				taskStatus.setStatus("started");
				taskStatus.setWorkflowId(workflowId);
				taskStatus.setTimestamp(System.currentTimeMillis());
				rabbit.send("bf.task_execution_status_notication", taskStatus);
				try {
					Thread.sleep(100);
				} catch (InterruptedException ie) {

				}

				taskStatus = new TaskStatusMessage();
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

				rabbit.send("bf.task_execution_status_notication", taskStatus);

			} else {
				throw new RuntimeException("Unexpected command " + op);
			}
		} else if (op.equals("test_cleanup")) {
			try {
				cleanupTest();
				rabbit.send("bf.test_cleanup_done", "");
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			throw new RuntimeException("Unexpected command " + op);
		}

	}

	private OtterMessage parseOp(String op, String payload)
			throws BadRequestException {
		OtterMessage msg;
		Class<OtterMessage> klass = opToArgs.get(op);
		if (klass == null) {
			throw new BadRequestException("Unknown op: " + op);
		}

		payload = payload.trim();
		if (payload.equals("")) {
			msg = null;
		} else {
			try {
				msg = mapper.readValue(payload, klass);
			} catch (IOException e1) {
				throw new BadRequestException(e1);
			}
		}

		long id = -1;
		if (op.equals("dataset_create")) {
			if (msg instanceof IdMessage) {
				return msg;
			}
			throw new BadRequestException("Invalid payload " + payload
					+ " for command " + op);
		} else if (op.equals("dataset_update")) {
			if (msg instanceof IdMessage) {
				return msg;
			}
			throw new BadRequestException("Invalid payload " + payload
					+ " for command " + op);
		} else if (op.equals("dataset_load")) {
			if (msg instanceof DatasetLoadMessage) {
				String mode = ((DatasetLoadMessage) msg).getMode();
				if (mode.equalsIgnoreCase("rewrite")
						|| mode.equalsIgnoreCase("append")) {

					return msg;
				}
				throw new BadRequestException("Invalid mode " + mode + " for "
						+ op);
			}
			throw new BadRequestException("Invalid payload " + payload
					+ " for command " + op);
		} else if (op.equals("task_run")) {
			if (msg instanceof IdMessage) {
				return msg;
			}
			throw new BadRequestException("Invalid payload " + payload
					+ " for command " + op);
		} else if (op.equals("test_cleanup")) {
			return msg;
		} else {

			throw new BadRequestException("Unknown op: " + op);
		}
	}

	@Override
	public void handleDelivery(String consumerTag, Envelope envelope,
			BasicProperties properties, byte[] body) throws IOException {
		// TODO Auto-generated method stub
		super.handleDelivery(consumerTag, envelope, properties, body);
		long deliveryTag = envelope.getDeliveryTag();
		String exchange = envelope.getExchange();
		String routingKey = envelope.getRoutingKey();
		String[] rKeyElts = routingKey.split("\\.");
		String direction = rKeyElts[0];
		String op = rKeyElts[1];
		String payload = new String(body);
		Logger.log("Received message on Exchange [" + exchange
				+ "] with routing key [" + routingKey + "]: " + payload);

		Channel ch = getChannel();
		if (direction.equals("fb")) {
			OtterMessage msg = null;
			try {
				msg = parseOp(op, payload);
				ch.basicAck(deliveryTag, false);
			} catch (BadRequestException ioe) {
				ioe.printStackTrace();
				ch.basicReject(deliveryTag, false);
				BadMessageError err = new BadMessageError();
				err.setDeliveryTag(deliveryTag);
				err.setOriginalMessage(payload);
				err.setReason(ioe.getCause().getMessage());
				rabbit.send("bf.error", err);
				return;
			}
			doOp(op, msg, deliveryTag);

		} else {
			Logger.log("Rejecting, not for us.");
			getChannel().basicReject(deliveryTag, false);
			return;
		}

		// mapper.readValue(src, valueType)

		//
	}
}
