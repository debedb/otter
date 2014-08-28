package com.enremmeta.otter.entity.messages;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TaskInfoResultSaved extends TaskInfo {

	public TaskInfoResultSaved() {
		// TODO Auto-generated constructor stub
	}
	
	@JsonProperty("result_tables")
	private List<TableMetaData> resultTables;

	public List<TableMetaData> getResultTables() {
		return resultTables;
	}

	public void setResultTables(List<TableMetaData> resultTables) {
		this.resultTables = resultTables;
	}

}
