package com.enremmeta.otter.entity.messages;

public class StatusMessage implements OtterMessage {

	public StatusMessage() {
		// TODO Auto-generated constructor stub
	}

	private boolean status;

	public boolean isStatus() {
		return status;
	}

	public void setStatus(boolean status) {
		this.status = status;
	}

	private String error;

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

}
