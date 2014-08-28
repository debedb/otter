package com.enremmeta.otter.entity.messages;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MetaData {

	public MetaData() {
		// TODO Auto-generated constructor stub
	}

	@JsonProperty("fields_count")
	private long fieldsCount;
	
	public long getFieldsCount() {
		return fieldsCount;
	}

	public void setFieldsCount(long fieldsCount) {
		this.fieldsCount = fieldsCount;
	}

	public long getRowsCount() {
		return rowsCount;
	}

	public void setRowsCount(long rowsCount) {
		this.rowsCount = rowsCount;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	@JsonProperty("rows_count")
	private long rowsCount;
	
	private long size;
}
