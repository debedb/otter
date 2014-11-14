package com.enremmeta.otter.rabbit;

import java.io.IOException;

import com.enremmeta.otter.Config;
import com.enremmeta.otter.Logger;
import com.enremmeta.otter.OtterException;
import com.enremmeta.otter.entity.messages.OtterMessage;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
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
	JsonFactory jsonFactory = new JsonFactory();
	jsonFactory.configure(Feature.ALLOW_SINGLE_QUOTES, true);
	mapper = new ObjectMapper(jsonFactory);
	mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    }

    private String exchange;

    private String queueIn;

    private String queueOut;

    private Channel channel;
    private Connection connection;

    public void send(String key, String msg) throws IOException {
	String logMsg = "Sending " + key + ": " + msg + ": ";
	try {
	    channel.basicPublish(exchange, key, null, msg.getBytes());
	    Logger.log(logMsg + "OK");
	} catch (IOException e) {
	    Logger.log(logMsg + "Failed (" + e.getMessage() + ")");
	    throw e;
	} catch (Exception e2) {
	    throw e2;
	}
    }

    private ObjectMapper mapper;

    public void send(String key, OtterMessage msg) throws IOException {
	String msgStr;
	msgStr = mapper.writeValueAsString(msg);
	send(key, msgStr);
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
	String host = Config.getInstance().getProperty("rabbit.host");
	factory.setHost(host);
	String user = Config.getInstance().getProperty("rabbit.username");
	factory.setUsername(user);
	factory.setPassword(Config.getInstance().getProperty("rabbit.password"));
	try {
	    connection = factory.newConnection();
	    channel = connection.createChannel();
	    exchange = Config.getInstance().getProperty("rabbit.exchange");
	    queueIn = Config.getInstance().getProperty("rabbit.queue_in");
	    queueOut = Config.getInstance().getProperty("rabbit.queue_out");
	    Logger.log("Connecting to as " + user + "@" + host + " to exchange "
		    + exchange + "; queue in: " + queueIn + "; queue out: "
		    + queueOut);
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
