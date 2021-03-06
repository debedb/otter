package com.enremmeta.otter.entity.messages;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TaskExecutionStatusNotification implements OtterMessage {

	public long getTaskId() {
		return taskId;
	}

	public void setTaskId(long taskId) {
		this.taskId = taskId;
	}

	public TaskExecutionStatusNotification() {
		// TODO Auto-generated constructor stub
	}

	@JsonProperty("meta_result")
	private List<MetaResult> metaResult;


	public List<MetaResult> getMetaResult() {
		return metaResult;
	}

	public void setMetaResult(List<MetaResult> metaResult) {
		this.metaResult = metaResult;
	}

	private String status;

	private TaskInfo info;

	@JsonProperty("executed_timestamp")
	private long timestamp;

	@JsonProperty("task_id")
	private long taskId;

	@JsonProperty("workflow_id")
	private long workflowId;

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public TaskInfo getInfo() {
		return info;
	}

	public void setInfo(TaskInfo info) {
		this.info = info;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public long getWorkflowId() {
		return workflowId;
	}

	public void setWorkflowId(long workflowId) {
		this.workflowId = workflowId;
	}

}
