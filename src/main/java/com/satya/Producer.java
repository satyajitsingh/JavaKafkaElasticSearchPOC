package com.satya;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.connect.json.JsonSerializer;

import java.util.Properties;
import java.util.Scanner;
import java.util.TimeZone;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.text.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

@SuppressWarnings("unused")
public class Producer {
	private static Scanner in;
	
	private static DateTimeFormatter dstf = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss",Locale.UK);
	
	public static void main(String[] args) throws ParseException {
		// TODO Auto-generated method stub
		if (args.length != 2) {
            System.err.println("Please specify 2 parameters: Topic and No. of Records to create ");
            System.exit(-1);
        }
        String topicName = args[0];
        int nRec = Integer.parseInt(args[1]);
        //in = new Scanner(System.in);
        System.out.println("Creating Records");
        //Configure the Producer
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.ByteArraySerializer");
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.connect.json.JsonSerializer");

        
		//org.apache.kafka.clients.producer.Producer producer = new KafkaProducer(configProperties);
		org.apache.kafka.clients.producer.Producer<String, JsonNode> producer = new KafkaProducer <String, JsonNode>(configProperties);
        ObjectMapper objectMapper = new ObjectMapper();
        LocalDateTime initDate = LocalDateTime.now();
        int ctr = 0;
        for(int i = 1; i<=nRec; i++) {
        	//System.out.println(initDate.format(dstf));
        	String toPort1 = "Moscow Russia";
        	String toPort2 = "Beijing China";
        	String toPort3 = "Colombo SriLanka";
        	String toPort4 = "NewDelhi India";
        	String fromPort1 = "Heathrow London";
        	String fromPort2 = "Luton London";
        	String fromPort3 = "Birmingham";
        	String fromPort4 = "Bristol";
        	
        	Contact contact = new Contact();
        	if (i % 6 == 0 ) {
        		 contact = new Contact(i,"Contact" + i,"Sname" + i,initDate.format(dstf), toPort1, fromPort1);
        	}
        	else if(i % 3 == 0) {
        		 contact = new Contact(i,"Contact" + i,"Sname" + i,initDate.format(dstf), toPort2, fromPort2);
        	}
        	else if(i % 4 == 0) {
        		 contact = new Contact(i,"Contact" + i,"Sname" + i,initDate.format(dstf), toPort3, fromPort3);
        	}
        	else if(i % 5 == 0) {
        		 contact = new Contact(i,"Contact" + i,"Sname" + i,initDate.format(dstf), toPort4, fromPort4);
        		initDate = initDate.plusDays(5);
        	}
        	else {
        		contact = new Contact(i,"Contact" + i,"Sname" + i,initDate.format(dstf), toPort1, fromPort1);
        	}
        	
        	/*test
        	try {
				System.out.println(objectMapper.writeValueAsString(contact));
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        	//end test */

        	try {
                JsonNode  jsonNode = objectMapper.valueToTree(contact);
                
                ProducerRecord<String, JsonNode> rec = new ProducerRecord<String, JsonNode>(topicName,jsonNode);
                producer.send(rec);
                
                }catch (Exception e) {
        			// TODO Auto-generated catch block
        			e.printStackTrace();
                }
                
        		
        	ctr = i;
        }
        
        System.out.println(Integer.toString(ctr) + " Records Created");
      /*  String line = in.nextLine();
        while(!line.equals("exit")) {
            Contact contact = new Contact(1,"satyajit","singh","21-11-2018 07:18:47");
            contact.parseString(line);
            try {
            JsonNode  jsonNode = objectMapper.valueToTree(contact);
            
            ProducerRecord<String, JsonNode> rec = new ProducerRecord<String, JsonNode>(topicName,jsonNode);
            producer.send(rec);
            line = in.nextLine();
            }catch (Exception e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    		}
        }
        in.close();*/
        producer.close();

	}

}
