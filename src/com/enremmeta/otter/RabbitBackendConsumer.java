package com.enremmeta.otter;

import java.io.IOException;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class RabbitBackendConsumer extends DefaultConsumer {

	private Rabbit rabbit;
	
	public RabbitBackendConsumer(Rabbit rabbit) {
		super(rabbit.getChannel());
		this.rabbit = rabbit;
	}
	
	//private String consumerTag ;
	
	@Override
	public void handleConsumeOk(String consumerTag) {
		// TODO Auto-generated method stub
		super.handleConsumeOk(consumerTag);
//		this.consumerTag = consumerTag;
	}
	
	

	@Override
	public void handleDelivery(String consumerTag, Envelope envelope,
			BasicProperties properties, byte[] body) throws IOException {
		// TODO Auto-generated method stub
		super.handleDelivery(consumerTag, envelope, properties, body);
		String exchange = envelope.getExchange();
		String routingKey = envelope.getRoutingKey();
		String payload = new String(body);
		Logger.log("Received message on Exchange [" + exchange + "] with routing key [" + routingKey + "]: " + payload);
	}

	

}
