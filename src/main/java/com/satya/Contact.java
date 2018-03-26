package com.satya;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.StringTokenizer;
import java.util.Locale;
import java.text.*;
import java.time.LocalDate;

import java.time.format.DateTimeFormatter;

public class Contact {
	private int contactId;
    private String firstName;
    private String lastName;
    private String travelDate;
    private String toPort;
    private String fromPort;
    
    DateTimeFormatter sdf = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS",Locale.UK);
    DateTimeFormatter dstf = DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss",Locale.UK);
    
    DateFormat df = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss",Locale.UK);

    
    public Contact(){

    }
    public Contact(int contactId, String firstName, String lastName, String travelDate, String toPort, String fromPort ) throws ParseException {
    	this.contactId = contactId;
        this.firstName = firstName;
        this.lastName = lastName;
        this.travelDate = travelDate; 
        this.toPort = toPort;
        this.fromPort = fromPort;
    }
    
    public void parseString(String csvStr) throws ParseException{
        StringTokenizer st = new StringTokenizer(csvStr,",");
        contactId = Integer.parseInt(st.nextToken());
        firstName = st.nextToken();
        lastName = st.nextToken();
        travelDate = st.nextToken();
        toPort = st.nextToken();
        fromPort = st.nextToken();
    }


    public int getContactId() {
        return contactId;
    }

    public void setContactId(int contactId) {
        this.contactId = contactId;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        this.firstName = firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        this.lastName = lastName;
    }
    
    public String getTravelDate() {
    	return travelDate;
    }
    
    public void setTravelDate(String travelDate) {
    	
		this.travelDate = travelDate; 
    }
    
    public String getToPort() {
    	return toPort;
    }
    
    public void setToPort(String toPort)  {
    	
		this.toPort = toPort; 
    }
    
    public String getFromPort() {
    	return fromPort;
    }
    
    public void setFromport(String fromport)  {
    	
		this.fromPort = fromport; 
    }
    
    @Override
    public String toString() {
        return "Contact{" +
                "contactId=" + contactId +
                ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' +
                ", travelDate='" + travelDate + '\'' +
                ", toPort='" + toPort + '\'' +
                ", fromPort='" + fromPort + '\'' +
                '}';
    }

    public static void main(String[] argv)throws Exception{
        ObjectMapper mapper = new ObjectMapper();
        Contact contact = new Contact();
        contact.setContactId(1);
        contact.setFirstName("Adam");
        contact.setLastName("John");
        contact.setTravelDate("2018-03-15T11:08:11.132");
        contact.setToPort("Beijing China");
        contact.setFromport("London Heathrow");
        System.out.println(mapper.writeValueAsString(contact));
        contact.parseString("2,Rahul,Sharma,21-09-2018 15:08:12,Colombo Srilanka,London Heathrow");
        System.out.println(mapper.writeValueAsString(contact));
    }
}
