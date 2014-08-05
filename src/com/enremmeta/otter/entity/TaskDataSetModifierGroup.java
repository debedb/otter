package com.enremmeta.otter.entity;

import java.io.Serializable;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonRootName;

@XmlRootElement
@JsonRootName(value = "")

public class TaskDataSetModifierGroup implements Serializable {
	public TaskDataSetModifierGroup() {
		super();
	}

	public TaskDataSetModifierGroup(long id) {
		super();
		this.id = id;
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
