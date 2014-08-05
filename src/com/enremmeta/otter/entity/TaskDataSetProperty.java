package com.enremmeta.otter.entity;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonRootName;

@XmlRootElement
@JsonRootName(value = "")
public class TaskDataSetProperty implements Serializable {

	private long id;
	
	private String alias;
	
	private String universalTitle;
	
	private String universalName;
	
	private String universalType;
	
	private String tableName;
	
	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getAlias() {
		return alias;
	}

	public void setAlias(String alias) {
		this.alias = alias;
	}

	public String getUniversalTitle() {
		return universalTitle;
	}

	public void setUniversalTitle(String universalTitle) {
		this.universalTitle = universalTitle;
	}

	public String getUniversalName() {
		return universalName;
	}

	public void setUniversalName(String universalName) {
		this.universalName = universalName;
	}

	public String getUniversalType() {
		return universalType;
	}

	public void setUniversalType(String universalType) {
		this.universalType = universalType;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String dbName) {
		this.tableName = dbName;
	}

	public TaskDataSetProperty() {
		super();
	}
	
	public TaskDataSetProperty(long id) {
		super();
		this.id = id;
	}
	
}
