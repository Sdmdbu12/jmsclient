/*
 ***************************************************************************
 *
 * Filename    : JMSMessageHandler
 * Author      : Bala Ramakrishnan
 * Date        : 2016-12-06
 * Description : JMS Client code example
 *
 ****************************************************************************
 */

package com.alu.cna.cloudmgmt.util.jms;

import java.util.Enumeration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.TextMessage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import org.json.simple.JSONObject;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
 
import java.io.OutputStream;
  
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

public class JMSMessageHandler implements MessageListener
{
    private static final Logger log = LoggerFactory.getLogger(JMSMessageHandler.class.getName());

    private ExecutorService taskPool;
    public JMSMessageHandler(ExecutorService taskPool)
    {
        this.taskPool = taskPool;
    }

    @Override
    public void onMessage(final Message msg)
    {
        try
        {
            taskPool.submit(new Callable<Boolean>()
            {
                @Override
                public Boolean call() throws Exception
                {
                    return processMessage(msg);
                }
            });
        }
        catch (RejectedExecutionException e)
        {
            log.error("error while submitting message task, message: {}", msg, e);
        }
    }

    protected boolean processMessage(Message msg)
    {
        // simply print the message for now
        log.debug("message type: {}", msg.getClass().getSimpleName());
        StringBuilder params = new StringBuilder("+++ Params:\n");
        try
        {
            @SuppressWarnings("unchecked")
            Enumeration<String> e = msg.getPropertyNames();
            while (e.hasMoreElements()) {
                String key = e.nextElement();
                params.append(key);
                params.append("=");
                params.append(msg.getObjectProperty(key));
                params.append("\n");
            }
            log.debug("message params: {}", params.toString());
            log.debug("message ID: {}", msg.getJMSMessageID());
			String msgString = null;
            if (msg instanceof TextMessage)
            {
				msgString = ((TextMessage) msg).getText();
                log.debug("message body: {}", msgString);
            }
            else
            {
				msgString = msg.toString();
                log.debug("message body: {}", msgString);
            }
			
            return sendToGraylog("http://172.16.0.53:12202/gelf", msgString);
        }
        catch (JMSException e)
        {
            log.error(e.getMessage(), e);
            return false;
        }
    }

	protected JSONObject getGelf(msgString)
	{
		JSONObject obj = new JSONObject();
		obj.put("version", "1.1");
		obj.put("host", "npi-p7-h1");
		obj.put("short_message", msgString);
		obj.put("timestamp", timestamp.getTime());
		return obj;
		
	}
	
	protected boolean sendToGraylog(targeturl, msgString)
	{
        JSONObject gelfjson = getGelf(msgString);
		URL myurl = new URL(targeturl);
        HttpURLConnection con = (HttpURLConnection)myurl.openConnection();
        con.setDoOutput(true);
        con.setDoInput(true);
 
        con.setRequestProperty("Content-Type", "application/json;");
        con.setRequestProperty("Accept", "application/json,text/plain");
        con.setRequestProperty("Method", "POST");
        OutputStream os = con.getOutputStream();
        os.write(gelfjson.toString().getBytes("UTF-8"));
        os.close();
 
 
        StringBuilder sb = new StringBuilder();  
        int HttpResult =con.getResponseCode();
        if(HttpResult ==HttpURLConnection.HTTP_OK){
			log.debug("Graylog send success");
			BufferedReader br = new BufferedReader(new   InputStreamReader(con.getInputStream(),"utf-8"));  
 
            String line = null;
            while ((line = br.readLine()) != null) {  
            sb.append(line + "\n");  
            }
             br.close();
			 log.debug(""+sb.toString());  
			return true;
        }else{
			log.debug("Graylog send Failed");
            log.debug(con.getResponseCode());
            log.debug(con.getResponseMessage());  
			return false;
        }  
	}
}
