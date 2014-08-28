package com.enremmeta.otter.entity.messages;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BadMessageError implements OtterMessage {

	public BadMessageError() {
		// TODO Auto-generated constructor stub
	}

	private String reason;
	
	@JsonProperty("delivery_tag")
	private long deliveryTag;
	
	public String getReason() {
		return reason;
	}

	public void setReason(String reason) {
		this.reason = reason;
	}

	public long getDeliveryTag() {
		return deliveryTag;
	}

	public void setDeliveryTag(long deliveryTag) {
		this.deliveryTag = deliveryTag;
	}

	public String getOriginalMessage() {
		return originalMessage;
	}

	public void setOriginalMessage(String originalMessage) {
		this.originalMessage = originalMessage;
	}

	@JsonProperty("original_message")
	private String originalMessage;
	
}
