package com.enremmeta.otter.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonRootName;

@XmlRootElement
@JsonRootName(value = "")
public class Task implements Serializable {

	public Task() {
		super();
	}

	public Task(long id) {
		super();
		this.id = id;
	}

	private long id;

	private String name;

	private TaskDataSet taskDataSet;

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	private List<TaskDataSet> datasets = new ArrayList<TaskDataSet>();

	public List<TaskDataSet> getDatasets() {
		return datasets;
	}

	public void setDatasets(List<TaskDataSet> datasets) {
		this.datasets = datasets;
	}

}
