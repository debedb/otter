package com.enremmeta.otter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Config {

	public static final String PROP_CDH_HOST = "cdh.host";

	public static final String PROP_CDH_UPLOAD_PATH = "cdh.upload_path";

	public static final String PROP_CDH_SSH_KEY_PATH = "cdh.ssh_key_path";

	public static final String PROP_CDH_SSH_USER = "cdh.ssh_user";

	public static final String PROP_CDH_UNIX_USER = "cdh.unix_user";

	public static final String PROP_MYSQL_HOST = "mysql.host";

	public static final String PROP_MYSQL_METADATA_DB = "mysql.metadata_db";
	public static final String PROP_MYSQL_REPORTING_DB = "mysql.reporting_db";

	public static final String PROP_MYSQL_PASS = "mysql.pass";

	public static final String PROP_MYSQL_USER = "mysql.user";

	public static final String PROP_DW_MGR_PORT = "dwmgr.port";

	private static Config config;

	public static Config getInstance() {
		if (config == null) {
			config = new Config();
		}
		return config;
	}

	private Config() {
		super();
	}

	public void load(File f) throws IOException {
		props = new Properties();
		props.load(new FileInputStream(f));
	}

	private Properties props;

	public String getProperty(String p) {
		String retval = props.getProperty(p);
		return retval;
	}
}
