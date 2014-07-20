package com.enremmeta.otter;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class OtterErrorResponseConverter {
	public OtterErrorResponseConverter() {
		super();
	}
	
	public OtterErrorResponseConverter(OtterException e) {
		super();
		this.errorMessage = e.getReason();
	}
	
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
