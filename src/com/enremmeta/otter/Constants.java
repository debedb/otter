package com.enremmeta.otter;

public interface Constants {
	String VERSION= "0.9";
	
	String DB_NAME = "x5";
	
	String IMPALA_HDFS_PREFIX = "/user/impala/";
	
	String HDFS_USER = "hdfs";
	
	String OTTER_HDFS_PREFIX = IMPALA_HDFS_PREFIX + DB_NAME + "/";
}
