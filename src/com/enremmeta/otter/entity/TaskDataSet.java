package com.enremmeta.otter.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonRootName;

@XmlRootElement
@JsonRootName(value = "")
public class TaskDataSet implements Serializable {

	public TaskDataSet() {
		super();
	}

    public TaskDataSet(long id) {
		super();
		this.id = id;
	}

	private long id;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	private String name;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	private List<TaskDataSetFilter> filters = new ArrayList<TaskDataSetFilter>();
	private List<TaskDataSetModifier> modifiers = new ArrayList<TaskDataSetModifier>();
	private List<TaskDataSetModifierGroup> groups = new ArrayList<TaskDataSetModifierGroup>();
	private List<TaskDataSetModifierSort> sorts = new ArrayList<TaskDataSetModifierSort>();
	private List<TaskDataSetProperty> fields = new ArrayList<TaskDataSetProperty>();
	
	public List<TaskDataSetProperty> getFields() {
		return fields;
	}

	public void setFields(List<TaskDataSetProperty> fields) {
		this.fields = fields;
	}

	public List<TaskDataSetModifier> getModifiers() {
		return modifiers;
	}

	public void setModifiers(List<TaskDataSetModifier> modifiers) {
		this.modifiers = modifiers;
	}

	public List<TaskDataSetModifierGroup> getGroups() {
		return groups;
	}

	public void setGroups(List<TaskDataSetModifierGroup> groups) {
		this.groups = groups;
	}

	public List<TaskDataSetModifierSort> getSorts() {
		return sorts;
	}

	public void setSorts(List<TaskDataSetModifierSort> sorts) {
		this.sorts = sorts;
	}

	public List<TaskDataSetFilter> getFilters() {
		return filters;
	}

	public void setFilters(List<TaskDataSetFilter> filters) {
		this.filters = filters;
	}
}
