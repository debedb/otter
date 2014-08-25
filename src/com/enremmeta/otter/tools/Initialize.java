package com.enremmeta.otter.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.AbstractHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ServerProperties;

import com.enremmeta.otter.ApiHandler;
import com.enremmeta.otter.CdhConnection;
import com.enremmeta.otter.Config;
import com.enremmeta.otter.Impala;
import com.enremmeta.otter.Logger;
import com.enremmeta.otter.OfficeDb;
import com.enremmeta.otter.OtterException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Initialize {

	
	public static void main(String[] args) throws Exception {
		File configFile = new File("config/x5.properties");
		if (args.length == 0) {
			Logger.log("Assuming config in " + configFile.getAbsolutePath());
		} else {
			configFile = new File(args[1]);
		}
		Logger.log("Reading properties from " + configFile.getAbsolutePath()
				+ "...");
		Config.getInstance().load(configFile);
	}
}
