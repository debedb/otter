package com.enremmeta.otter;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.InetAddress;
import java.net.InetSocketAddress;
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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ser.std.InetAddressSerializer;

public class Main extends AbstractHandler {

	private ApiHandler apiHandler = new ApiHandler();

	// TODO Auto-generated method stub
	@Override
	public void handle(String target, Request baseRequest,
			HttpServletRequest request, HttpServletResponse response)
			throws IOException, ServletException {
		String method = request.getMethod();
		String ct = request.getContentType();
		String path = request.getPathInfo();

		if (ct == null) {
			ct = "";
		}

		ct = ct.toLowerCase();

		// If GET
		if (method.equalsIgnoreCase("GET")) {
			// We don't care about content
		} else if (method.equalsIgnoreCase("POST")) {
			// Payload should be JSON
			if (ct.equals("") || ct.equals("application/json")) {
				BufferedReader br = new BufferedReader(new InputStreamReader(
						request.getInputStream()));
				String json = "";
				while (true) {
					String line = br.readLine();
					if (line == null) {
						break;
					}
					json += line;
				}
				String result = "";
				try {
					ObjectMapper mapper = new ObjectMapper();

					// Be forgiving...
					mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES,
							true);
					Map apiReq = mapper.readValue(json, Map.class);

					String[] pathElts = path.split("/");
					if (pathElts.length == 0) {

					}

					String action = apiReq.get("action").toString();

					String type = apiReq.get("type").toString();
					String methodName = "handle_" + action + "_" + type;

					Method handlerMethod = ApiHandler.class.getMethod(
							methodName, Object.class);
					result = (String) handlerMethod.invoke(this.apiHandler,
							apiReq);

					response.setContentType("text/html;charset=utf-8");
					response.setStatus(HttpServletResponse.SC_OK);
					baseRequest.setHandled(true);
					response.getWriter().println(result);
					return;
				} catch (Throwable e) {
					if (e instanceof InvocationTargetException) {
						e = ((InvocationTargetException) e)
								.getTargetException();
					}

					e.printStackTrace();
					response.setContentType("text/html;charset=utf-8");
					response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
					baseRequest.setHandled(true);
					response.getWriter().println(e.getMessage());
					return;
				}
			}
			response.setContentType("text/html;charset=utf-8");
			response.setStatus(HttpServletResponse.SC_NOT_FOUND);
			baseRequest.setHandled(true);
			response.getWriter().println("What are you trying to do?");
		}
	}

	private static void startServer() throws Exception {
		Config config = Config.getInstance();
		Logger.log("Port: " + config.getProperty(Config.PROP_OTTER_MGR_PORT));
		int port = Integer
				.parseInt(config.getProperty(Config.PROP_OTTER_MGR_PORT));

		// System.setProperty("jetty.host", "0.0.0.0");
		// System.setProperty("jetty.port", String.valueOf(port));
		 Server server = new Server(port);

		//InetSocketAddress inetAddr = InetSocketAddress.createUnresolved(
			//	"0.0.0.0", port);

		//Server server = new Server(new InetSocketAddress("54.235.199.212", port));
		//Server server = new Server(new InetSocketAddress("ec2-54-235-199-212.compute-1.amazonaws.com", port));
		
		ServletContextHandler context = new ServletContextHandler(
				ServletContextHandler.NO_SESSIONS);
		context.setContextPath("/");
		server.setHandler(context);

//		InetAddress[] addrs = InetAddress
//				.getAllByName("ec2-54-235-199-212.compute-1.amazonaws.com");
//		String[] vhosts = new String[addrs.length];
//		System.out.println("Hello dolly!!!");
//		for (int i = 0; i < addrs.length; i++) {
//			System.out.println(i);
//			System.out.println(addrs[i].getCanonicalHostName());
//			System.out.println(addrs[i].getHostAddress());
//			System.out.println(addrs[i].getHostName());
//			System.out.println(addrs[i].getAddress().toString());
//			System.out.println("Adding " + addrs[i].getHostName()
//					+ " to vhosts");
//			vhosts[i] = addrs[i].getHostName();
//		}
//
//		if (false) {
//			System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!");
//			server = new Server(new InetSocketAddress(addrs[0], port));
//			context = new ServletContextHandler(
//					ServletContextHandler.NO_SESSIONS);
//			context.setContextPath("/");
//			server.setHandler(context);
//		}

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

	private static void connectOfficeDb() throws Exception {
		OfficeDb odb = OfficeDb.getInstance();
		odb.connect();
		Logger.log("DB Connected.");
	}

	private static void setUp() throws Exception {
		// connectOfficeDb();
		CdhConnection.getInstance().connect();
		Impala.getInstance().connect();
		Logger.log("Connected to "
				+ Config.getInstance().getProperty(Config.PROP_CDH_HOST) + ".");
		Logger.log("Setup complete.");
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
		setUp();
		System.out.println("0.9.1");
		startServer();
	}
}
