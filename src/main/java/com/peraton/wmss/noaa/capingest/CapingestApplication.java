package com.peraton.wmss.noaa.capingest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.geojson.GeoJsonObject;
import org.geojson.Feature;
import org.geojson.FeatureCollection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.http.HttpStatus;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import java.io.*;
import java.util.List;
import java.util.Locale;
import java.text.ParseException;

@SpringBootApplication
public class CapingestApplication {

	public static void main(String[] args) throws IOException, ParseException {
		SpringApplication.run(CapingestApplication.class, args);
		DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd'T'HH:mm:ss").appendTimeZoneOffset("Z", true, 2, 4).toFormatter();
				DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss").withLocale(Locale.ROOT).withChronology(ISOChronology.getInstanceUTC());
		DateTime dtNow = new DateTime();
		CloseableHttpClient httpClient = HttpClients.createDefault();
		
		try {
			HttpGet request = new HttpGet("https://api.weather.gov/alerts?active=true&status=actual");
			request.addHeader("accept","application/json");
			CloseableHttpResponse response = httpClient.execute(request);
			try {
                // Get HttpResponse Status
                if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                    	String content = EntityUtils.toString(entity);
                    	ObjectMapper om = new ObjectMapper();
                    	JsonNode rootNode = om.readTree(content.getBytes());
                    	// Remove Json-ld nodes
                    	((ObjectNode) rootNode).remove("updated");
                    	((ObjectNode) rootNode).remove("title");
                    	((ObjectNode) rootNode).remove("@context");

                    	// convert remaining JSON to GeoJson.FeatureCollection
                    	InputStream geojsonStream = new ByteArrayInputStream(rootNode.toPrettyString().getBytes());
                    	FeatureCollection featureCollection = new ObjectMapper().readValue(geojsonStream, FeatureCollection.class);
                    	List<Feature> geojsonList = featureCollection.getFeatures();
                    	for( Feature aFeature : geojsonList ) {
                    		GeoJsonObject geom = aFeature.getGeometry();
                    		if (geom != null) {
                    			DateTime effDt = formatter.parseDateTime(aFeature.getProperty("effective"));
                    			DateTime expDt = formatter.parseDateTime(aFeature.getProperty("expires"));
                    			String cat = aFeature.getProperty("category");
                    			String sev = aFeature.getProperty("severity");
                    			String evt = aFeature.getProperty("event");
                    			String hdl = aFeature.getProperty("headline");
                    			String des = aFeature.getProperty("description");
                        		//System.out.print(hdl+"\n"+des+"\n\n");
                    			if(effDt.compareTo(dtNow) < 0 && expDt.compareTo(dtNow) > 0) {
                            		System.out.print(cat+": "+sev+" "+evt+" "+effDt.toString()+" - "+expDt.toString()+"\n\n");                    				
                    			}
                    		}
                    		/*
                    		Map<String, Object> geojsonMap = aFeature.getProperties();
                    		for( String propertyKey : geojsonMap.keySet()) {
                        		System.out.println(propertyKey);
                    		}
                    		*/
                    	}
                    }
                } else {
                	System.out.println("HTTP Response: "+response.getStatusLine().getStatusCode()+"\n"+response.getStatusLine().getReasonPhrase());
                }

            } catch (IOException e) {
            	e.printStackTrace();
            } finally {
                response.close();
            }
		} finally {
			httpClient.close();
		}
	}

	public static String convertDT(String aDT) {
		String dtArr[] = aDT.split("T");
		return dtArr[0]+" "+dtArr[1];
	}
}
