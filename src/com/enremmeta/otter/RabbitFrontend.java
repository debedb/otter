package com.enremmeta.otter;

import java.io.File;
import java.io.IOException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class RabbitFrontend implements Runnable {

	public static class FrontendConsumer extends DefaultConsumer {

		public FrontendConsumer(Channel channel) {
			super(channel);
		}
		
	//	private String consumerTag;

		@Override
		public void handleConsumeOk(String consumerTag) {
			// TODO Auto-generated method stub
			super.handleConsumeOk(consumerTag);
		//	this.consumerTag = consumerTag;
		}

		@Override
		public void handleDelivery(String consumerTag, Envelope envelope,
				BasicProperties properties, byte[] body) throws IOException {
			String exchange = envelope.getExchange();
			String routingKey = envelope.getRoutingKey();
			String payload = new String(body);
			Logger.log("FRONTEND: Received message on Exchange [" + exchange + "] with routing key [" + routingKey + "]: " + payload);
		}
	}

	public RabbitFrontend(boolean reader) {
		super();
		this.reader = reader;
	}

	private Rabbit rabbit = new Rabbit();

	public void connect() throws OtterException {
		rabbit.connect();
	}

	public void run() {
		try {
			rabbit.sendFb("fb.dataset_create", "{\"id\":6}");
		} catch (Exception oe) {
			oe.printStackTrace();
		}
	}

	private boolean reader;

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
		
		Rabbit frontendConsumerRabbit = new Rabbit();
		frontendConsumerRabbit.connect();
		Channel frontendChannel = frontendConsumerRabbit.getChannel();
			FrontendConsumer consumer = new FrontendConsumer(frontendChannel);
		frontendConsumerRabbit.getChannel().basicConsume(frontendConsumerRabbit.getQueueOut(), consumer);
		
		RabbitFrontend writer = new RabbitFrontend(false);
		writer.connect();
		Thread writerThread = new Thread(writer);
		writerThread.setName("FrontendConsumer");
		writerThread.start();
	}
}
