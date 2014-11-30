package com.enremmeta.otter.rabbit;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

import com.enremmeta.otter.BadRequestException;
import com.enremmeta.otter.Logger;
import com.enremmeta.otter.OtterException;
import com.enremmeta.otter.Utils;
import com.enremmeta.otter.Workhorse;
import com.enremmeta.otter.entity.messages.BadMessageError;
import com.enremmeta.otter.entity.messages.DatasetError;
import com.enremmeta.otter.entity.messages.DatasetSuccess;
import com.enremmeta.otter.entity.messages.IdMessage;
import com.enremmeta.otter.entity.messages.OtterMessage;
import com.enremmeta.otter.entity.messages.TaskExecutionStatusNotification;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
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

    private ObjectMapper mapper;

    private RabbitStatusHandler asyncStatusHandler;

    public RabbitConsumer(Rabbit rabbit) {
	super(rabbit.getChannel());
	// odb.set(new OfficeDb());
	// imp.set(new Impala());
	// cdhc.set(new CdhConnection());
	this.rabbit = rabbit;
	asyncStatusHandler = new RabbitStatusHandler(rabbit);
	this.workhorse = new Workhorse(asyncStatusHandler);
	mapper = new ObjectMapper();
	mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);

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
	try {
	    // TODO Auto-generated method stub
	    super.handleDelivery(consumerTag, envelope, properties, body);

	    long deliveryTag = envelope.getDeliveryTag();
	    String exchange = envelope.getExchange();
	    String routingKey = envelope.getRoutingKey();

	    String[] rKeyElts = routingKey.split("\\.");
	    if (rKeyElts.length != 2) {
		Logger.log("Ignoring, not for us: " + routingKey);
		return;
	    }
	    String direction = rKeyElts[0];
	    String op = rKeyElts[1];
	    String payload = new String(body);
	    Logger.log("Received message " + deliveryTag + " on Exchange ["
		    + exchange + "] with routing key [" + routingKey + "]: "
		    + payload);

	    Channel ch = getChannel();
	    if (direction.equals("fb")) {
		ch.basicAck(deliveryTag, false);
		OtterMessage msg = null;
		OtterMessage result = null;

		try {
		    Logger.log(payload);
		    result = workhorse.dispatch(op, payload);
		    if (result == null) {
			Logger.log("Nothing more to send for " + op + " and "
				+ payload);
			// because already emitted response.
			return;
		    }
		    OtterMessage reply = (OtterMessage) result;
		    // Temporary?
		    if (result instanceof DatasetSuccess) {
			DatasetSuccess ds = (DatasetSuccess) result;
			ds.setCommand(op);
			ds.setDeliveryTag(deliveryTag);
		    }

		    String key = "bf."
			    + Utils.underscore(reply.getClass().getSimpleName());
		    if (result.getClass().equals(IdMessage.class)) {
			Logger.log("FRONTEND not consuming messages now, but result was: "
				+ key + "," + mapper.writeValueAsString(reply));
		    } else {
			rabbit.send(key, reply);
		    }
		} catch (Throwable t) {
		    t.printStackTrace();
		    if (t instanceof BadRequestException) {
			BadRequestException bre = (BadRequestException) t;
			ch.basicReject(deliveryTag, false);
			BadMessageError err = new BadMessageError();
			err.setDeliveryTag(deliveryTag);
			err.setOriginalMessage(payload);
			if (bre.getCause() != null) {
			    err.setReason(bre.getCause().getMessage());
			} else {
			    err.setReason(bre.getMessage());
			}
			rabbit.send("bf.error", err);
			return;
		    } else {
			if (t instanceof InvocationTargetException) {
			    t = ((InvocationTargetException) t)
				    .getTargetException();
			}
			BadMessageError err2 = new BadMessageError();
			err2.setDeliveryTag(deliveryTag);
			err2.setOriginalMessage(payload);
			err2.setReason(t.getMessage());

			rabbit.send("bf.error", err2);
			return;
		    }
		}
	    } else {
		Logger.log("Ignoring, not for us: " + routingKey);
		return;
	    }
	} catch (Throwable ttt) {
	    ttt.printStackTrace();
	}
    }
}
