package com.enremmeta.otter.entity;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonRootName;

@XmlRootElement
@JsonRootName(value = "")
public class TaskDataSetModifier implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 5770455598665360940L;

	public TaskDataSetModifier() {
		super();
	}

	public TaskDataSetModifier(long id) {
		super();
		this.id = id;
	}

	private long id;
	private String alias;
	private String expression;

	private TaskDataSetProperty property;

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

	public String getExpression() {
		return expression;
	}

	public void setExpression(String expression) {
		this.expression = expression;
	}

	public TaskDataSetProperty getProperty() {
		return property;
	}

	public void setProperty(TaskDataSetProperty property) {
		this.property = property;
	}
}
