package com.enremmeta.otter.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.MediaType;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import com.enremmeta.otter.Logger;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestRest {

	private static Object runCommand(String jsonFilename, String id)
			throws Exception {
		File jsonFile = new File(jsonDir, jsonFilename);
		BufferedReader br = new BufferedReader(new FileReader(jsonFile));
		String json = "";
		while (true) {
			String line = br.readLine();
			if (line == null) {
				break;
			}
			if (line.startsWith("#")) {
				System.out.println("Skipping comment '" + line + "'");
				continue;
			}
			json += line;
		}
		String url = baseUrl;
		String[] restUrlElts = jsonFilename.replace(".json", "").split("_");
		String httpVerb = restUrlElts[1];
		String noun = restUrlElts[2];
		url += noun;
		String queryStr = "";
		for (int i = 3; i < restUrlElts.length; i++) {
			String elt = restUrlElts[i];
			if (elt.equals("ID")) {
				url += "/" + id;
			} else if (elt.startsWith("$")) {
				if (queryStr.length() > 0) {
					queryStr += "&";
				}
				queryStr += elt.substring(1) + "=" + restUrlElts[i + 1];
				i++;
			} else {
				url += "/" + elt;
			}
		}
		if (queryStr.length() > 0) {
			url += "?" + queryStr;
		}

		HttpRequestBase request = null;
		if (httpVerb.equalsIgnoreCase("POST")) {
			request = new HttpPost(url);
		} else if (httpVerb.equalsIgnoreCase("GET")) {
			request = new HttpGet(url);
		} else if (httpVerb.equalsIgnoreCase("PUT")) {
			request = new HttpPut(url);
		} else if (httpVerb.equalsIgnoreCase("DELETE")) {
			request = new HttpDelete(url);
		} else {
			throw new UnsupportedOperationException(httpVerb);
		}

		if (request instanceof HttpEntityEnclosingRequestBase) {
			request.addHeader("content-type", MediaType.APPLICATION_JSON);
			StringEntity body = new StringEntity(json);
			((HttpEntityEnclosingRequestBase) request).setEntity(body);
		}

		System.out.println("****** REQUEST ******");
		System.out.println(httpVerb.toUpperCase() + " " + url);
		System.out.println("");
		System.out.println(json);
		System.out.println("*********************");

		httpClient = new DefaultHttpClient();
		HttpResponse response = httpClient.execute(request);
		HttpEntity entity = response.getEntity();
		String responseTxt = "";
		if (entity != null) {
			InputStream is = entity.getContent();
			br = new BufferedReader(new InputStreamReader(is));
			while (true) {
				String line = br.readLine();
				if (line == null) {
					break;
				}
				responseTxt += line;
			}
		} else {
			Logger.log("<EMPTY BODY>");
		}

		StatusLine sl = response.getStatusLine();
		int code = sl.getStatusCode();
		System.out.println(code + " " + sl.getReasonPhrase());
		Object resp = null;

		ObjectMapper mapper = new ObjectMapper();
		mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
		System.out.println("****** RESPONSE *******");
		System.out.println(responseTxt);
		System.out.println("**********************");
		responseTxt = responseTxt.trim();
		if (responseTxt.length() == 0) {
			return null;
		}
		try {
			resp = mapper.readValue(responseTxt, Map.class);
		} catch (Exception e) {
			try {
				resp = mapper.readValue(responseTxt, List.class);
			} catch (Exception e2) {
				throw e2;
			}
		}

		return resp;
	}

	private static String baseUrl = "http://localhost:8088/";

	private static File jsonDir;
	private static HttpClient httpClient;

	public static void main(String[] a) throws Exception {
		if (a.length > 0) {
			baseUrl = a[0];
		}
		if (a.length > 1) {
			
		}
		String cwd = new File(".").getAbsolutePath();
		jsonDir = new File(cwd, "examples");

		List<String> jsonFiles = Arrays.asList(jsonDir.list());
		Collections.sort(jsonFiles);

		String id = null;
		for (String jsonFile : jsonFiles) {
			// Yeah yeah should have used a filter. But screw anonymous classes
			// instead of first-class functions. That's my protest!
			if (!jsonFile.endsWith(".json")) {
				System.err.println("Ignoring " + jsonFile);
				continue;
			}
			Logger.log("----------------------------------------");
			Logger.log("Processing "  + jsonFile);
			Logger.log("----------------------------------------");
			
			try {
				String filePrefix = jsonFile.split("_")[0];
				Long testNum = Long.valueOf(filePrefix);
				
				
			} catch (NumberFormatException nfe) {
				System.err.println("Ignoring file " + jsonFile);
				continue;
			}
			@SuppressWarnings("rawtypes")
			Object result = runCommand(jsonFile, id);

			if (result instanceof Map) {
				Object idObj = ((Map) result).get("id");
				String id2 = null;
				if (idObj != null) {
					id2 = String.valueOf(idObj);
				}
				if (id2 != null) {
					id = id2;
				}
			}
		}
	}
}
