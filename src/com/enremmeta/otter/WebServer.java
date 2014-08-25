package com.enremmeta.otter;

import java.io.File;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ServerProperties;

import com.rabbitmq.client.AMQP.Connection;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;

public class WebServer {

	private static void startServer() throws Exception {
		Config config = Config.getInstance();
		Logger.log("Port: " + config.getProperty(Config.PROP_OTTER_MGR_PORT));
		int port = Integer.parseInt(config
				.getProperty(Config.PROP_OTTER_MGR_PORT));

		// System.setProperty("jetty.host", "0.0.0.0");
		// System.setProperty("jetty.port", String.valueOf(port));
		Server server = new Server(port);

		// InetSocketAddress inetAddr = InetSocketAddress.createUnresolved(
		// "0.0.0.0", port);

		// Server server = new Server(new InetSocketAddress("54.235.199.212",
		// port));
		// Server server = new Server(new
		// InetSocketAddress("ec2-54-235-199-212.compute-1.amazonaws.com",
		// port));

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

		// server.setHandler(new Main());
		server.start();
		Logger.log("Listening on " + port + "...");
		server.join();
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