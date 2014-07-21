package com.enremmeta.otter.entity;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonRootName;

@XmlRootElement
@JsonRootName(value = "")
public class Query implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7393306828674490788L;

	public Query() {
		super();
	}

	private String query;

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

}
