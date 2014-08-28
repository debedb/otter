package com.enremmeta.otter.entity.messages;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DatasetSuccess extends IdMessage {

	public DatasetSuccess() {
		// TODO Auto-generated constructor stub
	}

	@JsonProperty("delivery_tag")
	private long deliveryTag;

	public long getDeliveryTag() {
		return deliveryTag;
	}

	public void setDeliveryTag(long deliveryTag) {
		this.deliveryTag = deliveryTag;
	}

	private String command;

	public String getCommand() {
		return command;
	}

	public void setCommand(String command) {
		this.command = command;
	}

}
