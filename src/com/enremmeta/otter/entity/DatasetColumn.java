package com.enremmeta.otter.entity;

public class DatasetColumn {
	private String type;
	private String name;
	private String fmt;
	private String title;

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getFmt() {
		return fmt;
	}

	public void setFmt(String fmt) {
		this.fmt = fmt;
	}

	@Override
	public String toString() {
		String retval = "DatasetColumn<Name: " + name + "; type: " + type;
		if (fmt != null) {
			retval += "; fmt: " + fmt;
		}
		return retval + ">";
	}
}
