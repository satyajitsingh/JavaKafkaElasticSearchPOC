package com.satya;

//import com.fasterxml.jackson.core.JsonProcessingException;

import java.util.Arrays;
//import java.util.Date;
import java.util.Properties;
import java.util.Scanner;

import org.apache.http.entity.StringEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.client.methods.HttpPut;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;


public class Consumer {

	 private static Scanner in;
	 //private static SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
	 //private static DateTimeFormatter dstf = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss",Locale.UK);
	 
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		if (args.length != 2) {
            System.err.printf("Usage: %s <topicName> <groupId>\n",
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
	}

	private static class ConsumerThread extends Thread{
		private String topicName;
        private String groupId;
        private KafkaConsumer<String,JsonNode> kafkaConsumer;

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
            //ObjectMapper mapper = new ObjectMapper();

            //Start processing messages
            try {
                while (true) {
                    ConsumerRecords<String, JsonNode> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, JsonNode> record : records) {
                        JsonNode jsonNode = record.value();
                        //System.out.println(mapper.treeToValue(jsonNode,Contact.class));
                        
                        //ss out to elastic search 
                        System.out.println("Start store in ELastic Search Process.");

                        	try {
                        		StringEntity  entity = new StringEntity(jsonNode.toString(),"UTF-8");
                        		JsonNode idNode = jsonNode.get("contactId");
                        		JsonNode dtNode = jsonNode.get("toPort");
                        		String url = "http://localhost:9200/" + groupId + "/record/" + idNode.toString() + "?pretty";
                        		//if match then																																																																																																																																																																																																																																																															
                        		if(dtNode.equals("Beijing China")){
                        	    	url = "http://localhost:9200/" + groupId + "/match/" + idNode.toString() + "?pretty";
                        		}
                        	    HttpPut put = new HttpPut(url);
                        	    put.setEntity(entity);

                        	    HttpClientBuilder clientBuilder = HttpClientBuilder.create();
                        	    HttpClient client = clientBuilder.build();

                        	    put.addHeader("Content-Type", "application/json");
                        	    

                        	    HttpResponse response = client.execute(put);
                        	    //System.out.println("Response: " + response);
                        	}
                        	catch(Exception ex) {
                       
                        		 ex.printStackTrace();
                        	}
                        	System.out.println("Finished store in ELastic Search process.");
                       // }
                    }
                }
            }
            catch(WakeupException ex){
                System.out.println("Exception caught " + ex.getMessage());
            } catch (Exception e) {
                e.printStackTrace();
            } finally{
                kafkaConsumer.close();
                System.out.println("After closing KafkaConsumer");
            }
        }
        public KafkaConsumer<String,JsonNode> getKafkaConsumer(){
            return this.kafkaConsumer;
        }
	}
}
