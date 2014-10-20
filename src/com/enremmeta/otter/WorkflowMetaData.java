package com.enremmeta.otter;

import com.enremmeta.otter.entity.messages.MetaData;
import com.enremmeta.otter.entity.messages.MetaResult;

public class WorkflowMetaData {

	public WorkflowMetaData() {
		// TODO Auto-generated constructor stub
	}

	private String sql;
	
	private MetaData metaData = new MetaData();
	
	private MetaResult metaResult = new MetaResult();

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}

	public MetaData getMetaData() {
		return metaData;
	}

	public void setMetaData(MetaData metaData) {
		this.metaData = metaData;
	}

	public MetaResult getMetaResult() {
		return metaResult;
	}

	public void setMetaResult(MetaResult metaResult) {
		this.metaResult = metaResult;
	}
}
