package com.enremmeta.otter.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Client {

	private static Map runCommand(String jsonFilename, String id)
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
		String url = "http://localhost:8088/";
		String restUrlElts[] = jsonFilename.replace(".json", "").split("_");

		String httpVerb = restUrlElts[1];
		String noun = restUrlElts[2];
		url += noun;
		for (int i = 3; i < restUrlElts.length; i++) {
			String elt = restUrlElts[i];
			if (elt.equals("ID")) {
				url += "/" + id;
			} else {
				url += "/" + elt;
			}
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

		System.out.println("Executing " + httpVerb.toUpperCase() + " " + url);
		
		HttpResponse response = httpClient.execute(request);
		HttpEntity entity = response.getEntity();
		String responseTxt = "";
		br = new BufferedReader(new InputStreamReader(entity.getContent()));
		while (true) {
			String line = br.readLine();
			if (line == null) {
				break;
			}
			responseTxt += line;
		}

		StatusLine sl = response.getStatusLine();
		int code = sl.getStatusCode();
		System.out.println(code + " " + sl.getReasonPhrase());
		Map resp = null;
		if (code >= 400) {
			System.out.println(responseTxt);
			System.out.println("**********************************");
		} else {
			ObjectMapper mapper = new ObjectMapper();
			mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
			resp = mapper.readValue(responseTxt, Map.class);
			System.out.println(resp);
		}
		return resp;
	}

	private static File jsonDir;
	private static HttpClient httpClient;

	public static void main(String[] a) throws Exception {

		httpClient = new DefaultHttpClient();
		String cwd = new File(".").getAbsolutePath();
		jsonDir = new File(cwd, "examples");

		List<String> jsonFiles = Arrays.asList(jsonDir.list());
		Collections.sort(jsonFiles);

		String id = null;
		for (String jsonFile : jsonFiles) {
			@SuppressWarnings("rawtypes")
			Map result = runCommand(jsonFile, id);
			String id2 = (String) result.get("id");
			if (id2 != null) {
				id = id2;
			}
		}
	}
}
