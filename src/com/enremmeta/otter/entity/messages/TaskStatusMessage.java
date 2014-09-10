package com.enremmeta.otter.entity.messages;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TaskStatusMessage  implements OtterMessage {

	public long getTaskId() {
		return taskId;
	}

	public void setTaskId(long taskId) {
		this.taskId = taskId;
	}

	public TaskStatusMessage() {
		// TODO Auto-generated constructor stub
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
