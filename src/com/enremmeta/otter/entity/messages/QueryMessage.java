package com.enremmeta.otter.entity.messages;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonRootName;

@XmlRootElement
@JsonRootName(value = "")
public class QueryMessage implements OtterMessage {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7393306828674490788L;

	public QueryMessage() {
		super();
	}

	private String query;

	private boolean save;
	
	public boolean isSave() {
		return save;
	}

	public void setSave(boolean save) {
		this.save = save;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

}
