package com.enremmeta.otter.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.enremmeta.otter.Config;
import com.enremmeta.otter.Logger;
import com.enremmeta.otter.OtterException;
import com.enremmeta.otter.rabbit.Rabbit;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class TestRabbit implements Runnable {

	public static List<String[]> TEST_COMMANDS = new ArrayList<String[]>();

	public static class FrontendConsumer extends DefaultConsumer {

		private int counter = 0;

		private Rabbit rabbit;

		public FrontendConsumer(Rabbit rabbit) {
			super(rabbit.getChannel());
			this.rabbit = rabbit;
		}

		// private String consumerTag;

		@Override
		public void handleConsumeOk(String consumerTag) {
			// TODO Auto-generated method stub
			super.handleConsumeOk(consumerTag);
			// this.consumerTag = consumerTag;
		}

		@Override
		public void handleDelivery(String consumerTag, Envelope envelope,
				BasicProperties properties, byte[] body) throws IOException {
			String exchange = envelope.getExchange();
			String routingKey = envelope.getRoutingKey();
			String payload = new String(body);
			Logger.log("FRONTEND: Received message on Exchange [" + exchange
					+ "] with routing key [" + routingKey + "]: " + payload);
			long deliveryTag = envelope.getDeliveryTag();
			getChannel().basicAck(deliveryTag, false);
			if (counter >= TEST_COMMANDS.size()) {
				// getChannel().basicCancel(getConsumerTag());
				Logger.log("DONE");
				return;
			}
			String[] newMsg = TEST_COMMANDS.get(counter++);
			rabbit.send(newMsg[0], newMsg[1]);
		}
	}

	public TestRabbit(boolean reader) {
		super();
		this.reader = reader;
	}

	private Rabbit rabbit = new Rabbit();

	public void connect() throws OtterException {
		rabbit.connect();
	}

	public void run() {
		try {
			Thread.sleep(1000);
		} catch (Exception oe) {
			oe.printStackTrace();
		}
	}

	private boolean reader;

	private static void loadTestData(File f) throws Exception {
		BufferedReader br = new BufferedReader(new FileReader(f));
		int lineCnt = -1;
		String curCmd = null;
		while (true) {

			lineCnt ++;
			String line = br.readLine();
			if (line == null) {
				break;
			}
			line = line.trim();
			if (line.equals("")) {
				continue;
			}
			
			if (lineCnt % 2 == 0) {
				curCmd = line;
			} else {
				String payload = line;
				TEST_COMMANDS.add(new String[] { curCmd, payload });
				curCmd = null;
			}
		}
	}

	public static void main(String[] args) throws Exception {
		File testData = new File(args[0]);
		loadTestData(testData);

		File configFile = new File("config/otter1.properties");
		if (args.length == 1) {
			Logger.log("Assuming config in " + configFile.getAbsolutePath());
		} else {
			configFile = new File(args[1]);
		}

		Logger.log("Reading properties from " + configFile.getAbsolutePath()
				+ "...");
		Config config = Config.getInstance();
		config.load(configFile);
		config.validate();

		Rabbit fRabbit = new Rabbit();
		fRabbit.connect();
		FrontendConsumer consumer = new FrontendConsumer(fRabbit);
		fRabbit.getChannel().basicConsume(fRabbit.getQueueOut(), consumer);

		// Kick this off...
//		fRabbit.send("fb.test_cleanup", "");
		fRabbit.send("fb.noop", "");

		TestRabbit f = new TestRabbit(false);
		f.connect();
		Thread t = new Thread(f);
		t.setName("Frontend");
		t.start();
		t.join();
	}
}
