package com.enremmeta.otter.entity;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

    private List<Task> subtasks = new ArrayList<Task>();

    public List<Task> getSubtasks() {
	return subtasks;
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

    private boolean complex = false;

    public boolean isComplex() {
	return complex;
    }

    public void setComplex(boolean complex) {
	this.complex = complex;
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

    @Override
    public String toString() {
	String retval = "";
	for (Field f : getClass().getDeclaredFields()) {
	    if (!Modifier.isStatic(f.getModifiers())) {
		if (retval.length() > 0) {
		    retval += "; ";
		}
		Object val = null;
		try {
		    val = f.get(this);
		} catch (Exception e) {
		    val = e;
		}
		retval += f.getName() + ": " + val;
	    }
	}

	retval += "<TASK " + retval + ">";
	return retval;
    }
}
