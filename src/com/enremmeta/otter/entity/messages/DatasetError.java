package com.enremmeta.otter.entity.messages;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DatasetError extends IdMessage {

	public DatasetError() {
		// TODO Auto-generated constructor stub
	}

	private String reason;

	@JsonProperty("delivery_tag")
	private long deliveryTag;

	public long getDeliveryTag() {
		return deliveryTag;
	}

	public void setDeliveryTag(long deliveryTag) {
		this.deliveryTag = deliveryTag;
	}

	private String command;

	public String getReason() {
		return reason;
	}

	public void setReason(String reason) {
		this.reason = reason;
	}

	public String getCommand() {
		return command;
	}

	public void setCommand(String command) {
		this.command = command;
	}

}
