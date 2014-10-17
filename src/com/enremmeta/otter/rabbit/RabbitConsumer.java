package com.enremmeta.otter.rabbit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.enremmeta.otter.Logger;
import com.enremmeta.otter.OtterException;
import com.enremmeta.otter.Utils;
import com.enremmeta.otter.Workhorse;
import com.enremmeta.otter.entity.messages.BadMessageError;
import com.enremmeta.otter.entity.messages.DatasetSuccess;
import com.enremmeta.otter.entity.messages.IdMessage;
import com.enremmeta.otter.entity.messages.OtterMessage;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

public class RabbitConsumer extends DefaultConsumer {

	private Rabbit rabbit;

	// private ThreadLocal<OfficeDb> odb = new ThreadLocal<OfficeDb>();
	//
	// private ThreadLocal<Impala> imp = new ThreadLocal<Impala>();
	//
	// private ThreadLocal<CdhConnection> cdhc = new
	// ThreadLocal<CdhConnection>();
	private Workhorse workhorse;

	public void connect() throws OtterException {
		workhorse.connect();
		rabbit.connect();
	}

	private RabbitStatusHandler asyncStatusHandler;
	
	public RabbitConsumer(Rabbit rabbit) {
		super(rabbit.getChannel());
		// odb.set(new OfficeDb());
		// imp.set(new Impala());
		// cdhc.set(new CdhConnection());
		this.rabbit = rabbit;
		asyncStatusHandler = new RabbitStatusHandler(rabbit);
		this.workhorse = new Workhorse(asyncStatusHandler);
		JsonFactory jsonFactory = new JsonFactory();
		jsonFactory.configure(Feature.ALLOW_SINGLE_QUOTES, true);
	}

	// private String consumerTag ;

	@Override
	public void handleConsumeOk(String consumerTag) {
		// TODO Auto-generated method stub
		super.handleConsumeOk(consumerTag);
		// this.consumerTag = consumerTag;
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
				Logger.log(payload);
				ch.basicAck(deliveryTag, false);
				OtterMessage result = workhorse.dispatch(op, payload);
				OtterMessage reply = (OtterMessage)result;
				// Temporary?
				if (result instanceof DatasetSuccess) {
					DatasetSuccess ds = (DatasetSuccess)result;
					ds.setCommand(op);
					ds.setDeliveryTag(deliveryTag);
				}
				String key = Utils.underscore(reply.getClass().getSimpleName());
				rabbit.send(key, reply);
			} catch (Throwable bre) {
				bre.printStackTrace();
				ch.basicReject(deliveryTag, false);
				BadMessageError err = new BadMessageError();
				err.setDeliveryTag(deliveryTag);
				err.setOriginalMessage(payload);
				err.setReason(bre.getCause().getMessage());
				rabbit.send("bf.error", err);
				return;
			}
		} else {
			Logger.log("Rejecting, not for us.");
			getChannel().basicReject(deliveryTag, false);
			return;
		}
	}
}
