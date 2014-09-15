package com.enremmeta.otter.entity.messages;

import java.util.List;
import java.util.ArrayList;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MetaResult implements OtterMessage {
	@JsonProperty("table_name")
	private String tableName;

	private List<Field> fields = new ArrayList<Field>();

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public List<Field> getFields() {
		return fields;
	}

	public void setFields(List<Field> fields) {
		this.fields = fields;
	}
}
