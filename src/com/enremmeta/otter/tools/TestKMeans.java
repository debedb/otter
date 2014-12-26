package com.enremmeta.otter.tools;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import com.enremmeta.otter.CdhConnection;
import com.enremmeta.otter.Config;
import com.enremmeta.otter.Logger;

public class TestKMeans {

    public TestKMeans() {
	// TODO Auto-generated constructor stub
    }

    public static void main(String[] args) throws Exception {
	File configFile = new File("config/otter.properties");
	if (args.length == 0) {
	    Logger.log("Assuming config in " + configFile.getAbsolutePath());
	} else {
	    configFile = new File(args[0]);
	}

	Logger.log("Reading properties from " + configFile.getAbsolutePath()
		+ "...");

	Config config = Config.getInstance();
	config.load(configFile);
	config.validate();
	CdhConnection cdhc = new CdhConnection();
	cdhc.connect();
	List<String> colNames = Arrays.asList(new String[] { "a1", "a2", "a3",
		"a4", "id", "label", "cluster" });

	List<String> catColNames = Arrays.asList(new String[] { "label",
		"cluster" });
	List<String> idColNames = Arrays.asList(new String[] { "id" });
	cdhc.runKMeans("iris", colNames, catColNames, idColNames);
    }
}
