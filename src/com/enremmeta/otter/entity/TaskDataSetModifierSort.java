package com.enremmeta.otter.entity;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonRootName;

@XmlRootElement
@JsonRootName(value = "")

public class TaskDataSetModifierSort implements Serializable {
	public TaskDataSetModifierSort() {
		super();
	}

	public TaskDataSetModifierSort(long id) {
		super();
		this.id = id;
	}
	
	private String direction;
	

	public String getDirection() {
		return direction;
	}

	public void setDirection(String direction) {
		this.direction = direction;
	}

	private long id;
	
	private TaskDataSetProperty property;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public TaskDataSetProperty getProperty() {
		return property;
	}

	public void setProperty(TaskDataSetProperty property) {
		this.property = property;
	}
}
