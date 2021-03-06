package com.enremmeta.otter.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonRootName;

@XmlRootElement
@JsonRootName(value="")
public class Dataset implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -1754653786876946481L;

	private String name;

	private List<DatasetColumn> columns = new ArrayList<DatasetColumn>();

	public Dataset() {
		super();
	}

	public Dataset(Map map) {
		super();
		this.name = (String) map.get("name");
		List cols = (List) map.get("columns");
		for (Object colObj : cols) {
			Map col = (Map) colObj;
			DatasetColumn column = new DatasetColumn();
			column.setName((String) col.get("name"));
			column.setType((String) col.get("type"));
			column.setFmt((String) col.get("fmt"));
			this.columns.add(column);
		}
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public List<DatasetColumn> getColumns() {
		return columns;
	}

	public void setColumns(List<DatasetColumn> columns) {
		this.columns = columns;
	}

}
