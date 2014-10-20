package com.enremmeta.otter;

import java.sql.SQLException;

public class BadRequestException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6249070698994894876L;
	private int status = 500;
	
	public int getStatus()  {
		return status;
	}
	
	public void setStatus(int status) {
		this.status = status;
	}
	
	public BadRequestException() {
		super();
	}

	public BadRequestException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		
	}

	public BadRequestException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	public BadRequestException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}
	
	private String entity;
	
	public String getEntity() {
		return entity;
	}

	public void setEntity(String entity) {
		this.entity = entity;
	}

	public BadRequestException(Throwable cause) {
		super(cause);
		if (cause instanceof SQLException) {
			SQLException sqle = (SQLException) cause;
			String msg = sqle.getMessage();
			String errTxt = "AnalysisException: Table already exists: ";
			if (msg.startsWith(errTxt)) {
				// Conflict
				status = 409;
				entity = msg.replace(errTxt, "");
			}
		}
	}

}
