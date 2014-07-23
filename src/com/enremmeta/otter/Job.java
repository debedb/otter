package com.enremmeta.otter;

import java.io.Serializable;

public abstract class Job implements Serializable {
	private String id;
	
	private static final int STATUS_IN_PROGRESS = 0;
	private static final int STATUS_SUCCESS = 1;
	private static final int STATUS_FAILURE = 2;
	
	private int status = STATUS_IN_PROGRESS;
	
	private String message;
	
	
	public int getStatus() {
		return status;
	}

	public void setStatus(int status) {
		this.status = status;
	}

	public String getMessage() {
		return message;
	}

	public void setMessage(String message) {
		this.message = message;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public Job() {
		super();
	}
	
}
