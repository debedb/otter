package com.enremmeta.otter;

import java.io.File;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ServerProperties;

public class WebServer {

	public static Workhorse getWorkhorse() {
		return workhorse;
	}

	private static final Workhorse workhorse = new Workhorse(null);

	public static Server startServer() throws Exception {
		Config config = Config.getInstance();
		Logger.log("Port: " + config.getProperty(Config.PROP_OTTER_MGR_PORT));
		int port = Integer.parseInt(config
				.getProperty(Config.PROP_OTTER_MGR_PORT));

		Server server = new Server(port);

		ServletContextHandler context = new ServletContextHandler(
				ServletContextHandler.NO_SESSIONS);
		context.setContextPath("/");
		server.setHandler(context);

		ServletHolder jerseyServlet = context.addServlet(
				org.glassfish.jersey.servlet.ServletContainer.class, "/*");
		jerseyServlet.setInitOrder(1);
		jerseyServlet.setInitParameter(ServerProperties.PROVIDER_PACKAGES,
				"com.enremmeta.otter");
		jerseyServlet.setInitParameter(
				"com.sun.jersey.api.json.POJOMappingFeature", "true");

		workhorse.connect();

		server.start();
		Logger.log("Listening on " + port + "...");
		return server;
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
		startServer();
	}
}