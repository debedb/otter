package com.enremmeta.otter.entity;

import java.util.HashMap;
import java.util.Map;

public class Algorithm {

    public Algorithm() {
	super();
    }

    private String name;

    private long id;

    private String process;

    public String getName() {
	return name;
    }

    public void setName(String name) {
	this.name = name;
    }

    public long getId() {
	return id;
    }

    public void setId(long id) {
	this.id = id;
    }

    public String getProcess() {
	return process;
    }

    public void setProcess(String process) {
	this.process = process;
    }
    
    private Map data = new HashMap();
    

    public Map getData() {
        return data;
    }

    public void setData(Map data) {
        this.data = data;
    }

}
