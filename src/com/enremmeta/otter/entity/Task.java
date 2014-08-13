package com.enremmeta.otter.entity;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

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

	private Map<Long, TaskDataSet> datasets = new HashMap<Long, TaskDataSet>();

	public Map<Long, TaskDataSet> getDatasets() {
		return datasets;
	}

	public void setDatasets(Map<Long, TaskDataSet> datasets) {
		this.datasets = datasets;
	}

	private Algorithm algorithm;

	public Algorithm getAlgorithm() {
		return algorithm;
	}

	public void setAlgorithm(Algorithm algorithm) {
		this.algorithm = algorithm;
	}
}
