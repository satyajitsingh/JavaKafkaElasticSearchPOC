package com.satya;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

//import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.client.transport.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.*;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig.Builder;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.*;
import org.elasticsearch.client.RestClient;
public class TravellerMatch {
	private static Scanner in;
	public static RestClient restClient() {
		
		return RestClient.builder(
			       new HttpHost("localhost", 9200, "http")).build();
	
	}
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args.length != 2) {
            System.err.printf("Usage: %s <topicName> <groupId>\n",
                    com.satya.TravellerMatch.class.getSimpleName());
            System.exit(-1);
        }
        in = new Scanner(System.in);
        String fieldName = args[0];
        String fieldValue = args[1];
		
		try {
			RestClient client = restClient();
			Map<String,String> paramMap = new HashMap<String,String>();
			//paramMap.put("q", "toPort:Bejing China");
			paramMap.put("q", fieldName + ":" + fieldValue);//"toPort:Bejing China"
			paramMap.put("pretty","true");
			Response response = client.performRequest("Get", "/test/record/_search",paramMap);
			
			System.out.println(EntityUtils.toString((response.getEntity())));
			
		}catch(IOException e) {
			e.printStackTrace();
		}
		catch(Exception ex) {
			ex.printStackTrace();
		}
	}

}
