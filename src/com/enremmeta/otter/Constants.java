package com.enremmeta.otter;

public interface Constants {
	String DB_NAME = "x5";
	
	String IMPALA_HDFS_PREFIX = "/user/impala/";
	
	String OTTER_HDFS_PREFIX = IMPALA_HDFS_PREFIX + DB_NAME + "/";
}
