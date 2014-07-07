package com.enremmeta.otter.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.InputStreamReader;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

public class Client {

	private static void runCommand(String jsonFilename) throws Exception {
		File jsonFile = new File(jsonDir, jsonFilename + ".json");
		BufferedReader br = new BufferedReader(new FileReader(jsonFile));
		String json = "";
		while (true) {
			String line = br.readLine();
			if (line == null) {
				break;
			}
			json += line;
		}
		String url = "http://localhost:8088/";
		String restUrlElts []= jsonFilename.split("_");
		
		String httpVerb = restUrlElts[1];
		String noun = restUrlElts[2];
		String verb = restUrlElts[3];
		
		HttpPost request = new HttpPost(url);
		request.addHeader("content-type", "application/json");
		
		StringEntity body = new StringEntity(json);
		request.setEntity(body);
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
		System.out.println(response.getStatusLine().getStatusCode() + " " + response.getStatusLine().getReasonPhrase());
		System.out.println(responseTxt);
		System.out.println("**********************************");

	}

	private static File jsonDir;
	private static HttpClient httpClient;

	public static void main(String[] a) throws Exception {

		httpClient = new DefaultHttpClient();
		String cwd = new File(".").getAbsolutePath();
		jsonDir = new File(cwd, "examples");

		System.out.println("Adding dataset");
		
		runCommand("001_put_dataset_create");
		runCommand("002_put_data_load");
		// handle response here...

	}
}
