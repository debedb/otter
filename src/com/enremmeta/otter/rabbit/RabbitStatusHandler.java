package com.enremmeta.otter.rabbit;

import java.io.IOException;

import com.enremmeta.otter.AsyncStatusHandler;
import com.enremmeta.otter.Utils;
import com.enremmeta.otter.entity.messages.OtterMessage;

public class RabbitStatusHandler implements AsyncStatusHandler {

	public RabbitStatusHandler(Rabbit rabbit) {
		super();
		this.rabbit = rabbit;
	}

	private Rabbit rabbit;

	@Override
	public void handle(OtterMessage msg) {
		String key = msg.getClass().getSimpleName();
		key = Utils.underscore(key);
		key = "bf." + key;
		try {
			rabbit.send(key, msg);
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
	}
}
