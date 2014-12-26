package com.enremmeta.otter;

public class DistcpJob extends Job {
    public DistcpJob() {
	super();
    }

    private String localFile;

    private String bucket;

    private String table;

    public String getLocalFile() {
	return localFile;
    }

    public void setLocalFile(String localFile) {
	this.localFile = localFile;
    }

    public String getBucket() {
	return bucket;
    }

    public void setBucket(String bucket) {
	this.bucket = bucket;
    }

    public String getTable() {
	return table;
    }

    public void setTable(String table) {
	this.table = table;
    }

    public String getFullCommand() {
	return fullCommand;
    }

    public void setFullCommand(String fullCommand) {
	this.fullCommand = fullCommand;
    }

    private String fullCommand;

}
