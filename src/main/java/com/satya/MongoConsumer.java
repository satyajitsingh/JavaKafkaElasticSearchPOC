package com.satya;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
//import com.mongodb.DBCollection;
//import com.mongodb.Mongo;
import com.mongodb.MongoClient;
//import com.mongodb.MongoClientURI;
//import com.mongodb.ServerAddress;
//import com.mongodb.MongoCredential;
//import com.mongodb.MongoClientOptions;

public class MongoConsumer {

		// TODO Auto-generated method stub
		private static Scanner in;
		

		public static void main(String[] args) {
			try {
			// TODO Auto-generated method stub
			if (args.length != 2) {
	            System.err.printf("Usage: %s <DBName> <Collection>\n",
	                    com.satya.Consumer.class.getSimpleName());
	            System.exit(-1);
	        }
	        in = new Scanner(System.in);
	        String topicName = args[0];
	        String groupId = args[1];

	        ConsumerThread consumerRunnable = new ConsumerThread(topicName,groupId);
	        consumerRunnable.start();
	        String line = "";
	        while (!line.equals("exit")) {
	            line = in.next();
	        }
	        consumerRunnable.getKafkaConsumer().wakeup();
	        System.out.println("Stopping consumer .....");
	        try {
				consumerRunnable.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			}catch(Exception ex) {
				ex.printStackTrace();
			}
			
		}
		
		private static class ConsumerThread extends Thread{
			private String topicName;
	        private String groupId;
	        private KafkaConsumer<String,JsonNode> kafkaConsumer;
	        private MongoClient mongo;
	        private DBCollection collection;

	        public ConsumerThread(String topicName, String groupId){
	            this.topicName = topicName;
	            this.groupId = groupId;
	        }
	        @SuppressWarnings("unlikely-arg-type")
			public void run() {
	            Properties configProperties = new Properties();
	            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
	            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer");
	            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.connect.json.JsonDeserializer");
	            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
	            configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");


	            //Figure out where to start processing messages from
	            kafkaConsumer = new KafkaConsumer<String, JsonNode>(configProperties);
	            kafkaConsumer.subscribe(Arrays.asList(topicName));
	           
	            //connect to mongodb
	            try {
				mongo = new MongoClient("localhost",27017);
	            @SuppressWarnings("deprecation")
				DB db = mongo.getDB("testdb");
	            collection = db.getCollection("travel");
	            }catch(Exception ex) {
	            	ex.printStackTrace();
	            }
	            
	            ObjectMapper mapper = new ObjectMapper();
	            if(mongo !=null) {
	            //Start processing messages
	            try {
	            	 //while (true) {
	                	//out to Mongo DB
	                    ConsumerRecords<String, JsonNode> records = kafkaConsumer.poll(1000);
	                    if(records.count() > 0) {
	                    	System.out.println(records.count() +" found.Start store in Mongo Db.");
	                    
		                    for (ConsumerRecord<String, JsonNode> record : records) {
		                        JsonNode jsonNode = record.value();
		                        	try {
		                        		BasicDBObject document = new BasicDBObject();
		                        		//convert JSON 
		                        		Map<String, Object> bson = mapper.convertValue(jsonNode, Map.class);
		                        		document.putAll(bson);
		                        		collection.insert(document);
		                        	}
		                        	catch(Exception ex) {	                       
		                        		 ex.printStackTrace();
		                        	}		                    
		                    }
		                    System.out.println("Finished storing in Mongo Db.");
	                    }
	                    else {
	                    	System.out.println("NO Records to process ");
	                    }
	                    
	                //}
	            }
	            catch(WakeupException ex){
	                System.out.println("Exception caught " + ex.getMessage());
	            } catch (Exception e) {
	                e.printStackTrace();
	            } finally{
	                kafkaConsumer.close();
	                mongo.close();
	                System.out.println("Afterclosing KafkaConsumer");
	             }
	            }
	            else {
		        	System.out.println("Could not connect to MangoDb");
		        }
	        }
	        
	        public KafkaConsumer<String,JsonNode> getKafkaConsumer(){
	            return this.kafkaConsumer;
	        }
		}
}
