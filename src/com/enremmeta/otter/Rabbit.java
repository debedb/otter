package com.enremmeta.otter;

import java.io.IOException;

import org.apache.http.annotation.ThreadSafe;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class Rabbit {

	public String getExchange() {
		return exchange;
	}

	public void setExchange(String exchange) {
		this.exchange = exchange;
	}

	public String getQueueIn() {
		return queueIn;
	}

	public void setQueueIn(String queueIn) {
		this.queueIn = queueIn;
	}

	public String getQueueOut() {
		return queueOut;
	}

	public void setQueueOut(String queueOut) {
		this.queueOut = queueOut;
	}

	public Channel getChannel() {
		return channel;
	}

	public void setChannel(Channel channel) {
		this.channel = channel;
	}

	public Connection getConnection() {
		return connection;
	}

	public void setConnection(Connection connection) {
		this.connection = connection;
	}

	public Rabbit() {
		super();
	}

	private String exchange;

	private String queueIn;

	private String queueOut;

	private Channel channel;
	private Connection connection;

	public void sendFb(String key, String msg) throws OtterException {
		try {
			channel.basicPublish(exchange, key, null, msg.getBytes());
		} catch (IOException e) {
			throw new OtterException(e);
		}
	}

	public void sendBf(String key, String msg) throws OtterException {
		try {
			channel.basicPublish(exchange, key, null, msg.getBytes());
		} catch (IOException e) {
			throw new OtterException(e);
		}
	}
	
	
	public void x() throws OtterException {
		try {
		} catch (Exception e) {
			throw new OtterException(e);
		}
	}

	public synchronized void connect() throws OtterException {
		if (connection != null) {
			return;
		}
		ConnectionFactory factory = new ConnectionFactory();

		factory.setHost(Config.getInstance().getProperty("rabbit.host"));
		factory.setUsername(Config.getInstance().getProperty("rabbit.username"));
		factory.setPassword(Config.getInstance().getProperty("rabbit.password"));
		try {
			connection = factory.newConnection();
			channel = connection.createChannel();
			exchange = Config.getInstance().getProperty("rabbit.exchange");
			queueIn = Config.getInstance().getProperty("rabbit.queue_in");
			queueOut = Config.getInstance().getProperty("rabbit.queue_out");
			
			channel.exchangeDeclarePassive(exchange);
			channel.queueDeclarePassive(queueIn);
			channel.queueBind(queueIn, exchange, "");
			channel.queueDeclarePassive(queueOut);
			channel.queueBind(queueOut, exchange, "");

		} catch (IOException e) {
			throw new OtterException(e);
		}
	}
}
