package com.peraton.wmss.noaa.capingest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import org.geojson.GeoJsonObject;
import org.geojson.Feature;
import org.geojson.Polygon;
import org.geojson.FeatureCollection;
import org.geojson.LngLatAlt;

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
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import org.apache.qpid.jms.JmsConnectionFactory;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.TextMessage;
import javax.jms.MessageProducer;

import java.io.*;
import java.util.List;
import java.util.Locale;
import java.text.ParseException;

@SpringBootApplication
public class CapingestApplication {

	static Session ivSession;
	static MessageProducer ivProducer;

	/*
	 * Constructor - throws JMS Exception
	 */
	public CapingestApplication () throws JMSException {
		String queueName = System.getenv("QUEUENAME");
		String aURL = System.getenv("ARTEMISURL");
		String userName = System.getenv("USERNAME");
		String passWord = System.getenv("PASSWORD");
		try {
			JmsConnectionFactory factory = new JmsConnectionFactory(aURL);
			Connection connection = factory.createConnection(userName, passWord);
			connection.start();

			ivSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

			Destination destination = null;
			destination = ivSession.createQueue(queueName);	
			_setProducer(ivSession, destination);
		} catch( JMSException e) {
		    e.printStackTrace();
		}
		
	}

	
	public static void main(String[] args) throws InterruptedException {
		SpringApplication.run(CapingestApplication.class, args);
        
		DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd'T'HH:mm:ss").appendTimeZoneOffset("Z", true, 2, 4).toFormatter();
		DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss").withLocale(Locale.ROOT).withChronology(ISOChronology.getInstanceUTC());

		String delayStr = System.getenv("DELAYTIMESECS");
		int delaySeconds = Integer.parseInt(delayStr);
		
		while (true) {
			try {
				DateTime now = new DateTime();
				downloadAndParseCAP();
				System.out.print("Sleep "+delayStr+" - "+now.toString(formatter)+"\n");
				Thread.sleep(delaySeconds*1000);
			} catch (IOException e) {
				e.printStackTrace();
			} catch (JMSException e) {				
				e.printStackTrace();
			} catch (ParseException e) {				
			} finally {
			}
		}
	}

	
	
	/*
	 * Retrieves CAP JSON from weather.gov; parses JSON; queues messages to 
	 * activemq
	 */
	static void downloadAndParseCAP () throws IOException, ParseException, JMSException {
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
							//String featureId = aFeature.getId();
							if (geom != null) {
								DateTime effDt = formatter.parseDateTime(aFeature.getProperty("effective"));
								DateTime expDt = formatter.parseDateTime(aFeature.getProperty("expires"));
								String cat = aFeature.getProperty("category");
								String sev = aFeature.getProperty("severity");
								String evt = aFeature.getProperty("event");
								//String hdl = aFeature.getProperty("headline");
								//String des = aFeature.getProperty("description");
								if(effDt.compareTo(dtNow) < 0 && expDt.compareTo(dtNow) > 0) {
									System.out.print(cat+": "+sev+" "+evt+" "+effDt.toString()+" - "+expDt.toString()+"\n\n");                    				
									if(geom instanceof Polygon) {
										String jsonStr = "category:"+cat+";severity:"+sev+";effective:"+evt+";expires:"+effDt.toString()+";"+expDt.toString()+";{";
										List<LngLatAlt> lnglataltList = ((Polygon) geom).getExteriorRing();
										for(LngLatAlt lla : lnglataltList) {
											double longitude = lla.getLongitude();
											double latitude = lla.getLatitude();
											jsonStr = jsonStr+longitude+","+latitude+",";
										}
										jsonStr = jsonStr+"}";
							            TextMessage msg = ivSession.createTextMessage(jsonStr);
							            _getProducer().send(msg);
									}
								}
							}
						}
					}
				} else {
					System.out.println("HTTP Response: "+response.getStatusLine().getStatusCode()+"\n"+response.getStatusLine().getReasonPhrase());
				}
			} finally {
				response.close();
			}
		} finally {
			httpClient.close();
		}
	}

	private void _setProducer(Session aSession, Destination aDestination)  throws JMSException {
		if( ivProducer == null) {
			ivProducer = aSession.createProducer(aDestination);
		}
	}
	
	private static MessageProducer _getProducer() { return ivProducer; }
	
}
