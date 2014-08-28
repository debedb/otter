package com.enremmeta.otter.entity.messages;

import java.util.List;

public class DatasetLoadMessage implements OtterMessage {

	public DatasetLoadMessage() {
		// TODO Auto-generated constructor stub
	}

	private long id;

	private String mode;

	private List<DatasetLoadSource> sources;

	public List<DatasetLoadSource> getSources() {
		return sources;
	}

	public void setSources(List<DatasetLoadSource> sources) {
		this.sources = sources;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getMode() {
		return mode;
	}

	public void setMode(String mode) {
		this.mode = mode;
	}

}
