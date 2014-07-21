package com.enremmeta.otter;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class OtterErrorResponseConverter {
	public OtterErrorResponseConverter() {
		super();
	}
	
	public OtterErrorResponseConverter(OtterException e) {
		super();
		this.errorMessage = e.getMessage();
		this.errorClass = e.getClass().getCanonicalName();
		Throwable cause = e.getCause();
		if (cause != null) {
			causeMessage = cause.getMessage();
			causeClass = cause.getClass().getCanonicalName();
		}
	}

	private String errorClass;
	
	private String causeClass;
	private String causeMessage;
	
	
	private String errorMessage;
	
	private String entity;
	
	private int id;
	
	public String getEntity() {
		return entity;
	}

	public void setEntity(String entity) {
		this.entity = entity;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public OtterErrorResponseConverter(String msg) {
		this.errorMessage = msg;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setErrorMessage(String msg) {
		this.errorMessage = msg;
	}
}
