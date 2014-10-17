package com.enremmeta.otter;

import java.io.File;

import com.enremmeta.otter.rabbit.Rabbit;
import com.enremmeta.otter.rabbit.RabbitConsumer;
import com.rabbitmq.client.Channel;

public class Server implements Runnable {

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

	private CdhConnection cdhc = new CdhConnection();

	private Rabbit rabbit = new Rabbit();

	public void connect() throws Exception {
		// odb.connect();
		// cdhc.connect();
		// impala.connect();
		// rabbit.connect();
	}

	public static void main(String[] args) {
		try {
			File configFile = new File("config/otter.properties");
			if (args.length == 0) {
				Logger.log("Assuming config in " + configFile.getAbsolutePath());
			} else {
				configFile = new File(args[0]);
			}

			Logger.log("Reading properties from "
					+ configFile.getAbsolutePath() + "...");

			Config config = Config.getInstance();
			config.load(configFile);
			config.validate();

			WebServer.startServer();

			Rabbit consumerRabbit = new Rabbit();
			consumerRabbit.connect();
			RabbitConsumer consumer = new RabbitConsumer(
					consumerRabbit);
			consumer.connect();
			Channel backendChannel = consumerRabbit.getChannel();
			backendChannel.basicConsume(consumerRabbit.getQueueIn(), consumer);

			Server server = new Server();

			Thread thread = new Thread(server);
			thread.setName("BackendServer");
			thread.start();

			Logger.log("Ready...");
			thread.join();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}
}
