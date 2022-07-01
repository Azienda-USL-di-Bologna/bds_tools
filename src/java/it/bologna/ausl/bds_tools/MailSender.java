/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package it.bologna.ausl.bds_tools;

import java.util.Properties;
import javax.mail.Address;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

/**
 *
 * @author Fayssel
 */
public class MailSender {
    private String[] to;
    private String from;
    private String host;
    private Properties properties;
    private Session session;
    private String subject;
    private String messageBody;
    private int port;
    
    public MailSender(String[] to, String from, String host, String subject, String messageBody, int port){
        this.to = to;
        this.from =  from;
        this.host =  host;
        this.subject =  subject;
        this.messageBody = messageBody;
        properties = System.getProperties();
        this.port = port;
        properties.setProperty("mail.smtp.host", host);
        properties.setProperty("mail.smtp.port", String.valueOf(port));
        session = Session.getDefaultInstance(properties);
        
    }
    
    public void send() throws MessagingException{
        MimeMessage message = new MimeMessage(session);
        message.setFrom(new InternetAddress(from));
        message.addRecipients(Message.RecipientType.TO, fillAdresses(to));
        message.setSubject(subject);
        message.setContent(messageBody, "text/html");
        Transport.send(message);
    }
    
    private Address[] fillAdresses(String[] to) throws AddressException{
        Address[] addresses = new Address[to.length];
        for (int i = 0; i < to.length; i++) {
            addresses[i] = new InternetAddress(to[i]);
        }
        return addresses;
    }
}
