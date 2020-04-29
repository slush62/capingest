package com.peraton.wmss.noaa.capingest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
// import com.google.gson.*;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;

@SpringBootApplication
public class CapingestApplication {

	public static void main(String[] args) throws IOException {
		SpringApplication.run(CapingestApplication.class, args);
		
		CloseableHttpClient httpClient = HttpClients.createDefault();
		
		try {
			HttpGet request = new HttpGet("https://api.weather.gov/alerts?active=true&status=actual");
			CloseableHttpResponse response = httpClient.execute(request);
			response.close();
		} finally {
			httpClient.close();
		}
	}

}
