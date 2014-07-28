package com.enremmeta.otter.entity;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonRootName;

@XmlRootElement
@JsonRootName(value = "")
public class LoadSource implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -7393306828674490788L;

	public LoadSource() {
		super();
	}

	public LoadSource(Map m) {
		super();
	}

	private String path;
	
	private String delim = ",";

	public String getDelim() {
		return delim;
	}

	public void setDelim(String delim) {
		this.delim = delim;
	}

	private int location;

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public int getLocation() {
		return location;
	}

	public void setLocation(int location) {
		this.location = location;
	}
}
