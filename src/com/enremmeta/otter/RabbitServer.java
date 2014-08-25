package com.enremmeta.otter;

import java.io.File;

import com.rabbitmq.client.Channel;

public class RabbitServer implements Runnable {

	@Override
	public void run() {
		while (true) {
			try {
				Thread.sleep(100);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private OfficeDb odb = new OfficeDb();

	private Impala impala = new Impala();

	private CdhConnection cdhc = new CdhConnection();

	private Rabbit rabbit = new Rabbit();

	public void connect() throws Exception {
		odb.connect();
		cdhc.connect();
		impala.connect();
		rabbit.connect();
	}

	public static void main(String[] args) throws Exception {
		File configFile = new File("config/otter.properties");
		if (args.length == 0) {
			Logger.log("Assuming config in " + configFile.getAbsolutePath());
		} else {
			configFile = new File(args[0]);
		}

		Logger.log("Reading properties from " + configFile.getAbsolutePath()

		+ "...");

		Config config = Config.getInstance();
		config.load(configFile);
		config.validate();

		Rabbit consumerRabbit = new Rabbit();
		consumerRabbit.connect();
		RabbitBackendConsumer consumer = new RabbitBackendConsumer(
				consumerRabbit);
		Channel backendChannel = consumerRabbit.getChannel();
		backendChannel.basicConsume(consumerRabbit.getQueueIn(), consumer);

		RabbitServerProducer producer = new RabbitServerProducer();
		producer.connect();

		Thread pThread = new Thread(producer);
		pThread.setName("BackendProducer");
		pThread.join();
		pThread.start();
	}
}
