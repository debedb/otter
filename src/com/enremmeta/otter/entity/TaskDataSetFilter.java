package com.enremmeta.otter.entity;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonRootName;

@XmlRootElement
@JsonRootName(value = "")
public class TaskDataSetFilter implements Serializable {

	private String expression;

	private String type;

	private String value;

	private String isNotNull;

	private String connectOperator;

	private int parameterId;

	private TaskDataSet taskDataSet;

	private TaskDataSetProperty taskDataSetProperty;

	public TaskDataSetFilter() {
		super();
	}

	public TaskDataSetFilter(long id) {
		super();
		this.id = id;
	}

	public String getExpression() {
		return expression;
	}

	public void setExpression(String expression) {
		this.expression = expression;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getIsNotNull() {
		return isNotNull;
	}

	public void setIsNotNull(String isNotNull) {
		this.isNotNull = isNotNull;
	}

	public String getConnectOperator() {
		return connectOperator;
	}

	public void setConnectOperator(String connectOperator) {
		this.connectOperator = connectOperator;
	}

	public int getParameterId() {
		return parameterId;
	}

	public void setParameterId(int parameterId) {
		this.parameterId = parameterId;
	}

	public String getParameterName() {
		return parameterName;
	}

	public void setParameterName(String parameterName) {
		this.parameterName = parameterName;
	}

	private String parameterName;

	private long id;

	public TaskDataSet getTaskDataSet() {
		return taskDataSet;
	}

	public void setTaskDataSet(TaskDataSet taskDataSet) {
		this.taskDataSet = taskDataSet;
	}

	public TaskDataSetProperty getTaskDataSetProperty() {
		return taskDataSetProperty;
	}

	public void setTaskDataSetProperty(TaskDataSetProperty taskDataSetProperty) {
		this.taskDataSetProperty = taskDataSetProperty;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

}
