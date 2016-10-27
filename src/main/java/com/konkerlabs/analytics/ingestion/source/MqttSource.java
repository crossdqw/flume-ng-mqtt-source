package com.konkerlabs.analytics.ingestion.source;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import com.google.gson.reflect.TypeToken;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.eclipse.paho.client.mqttv3.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Type;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by renatoochando on 26/10/16.
 */
public class MqttSource extends AbstractSource implements Configurable, EventDrivenSource, MqttCallback {

    private static final Logger LOG = LoggerFactory.getLogger(MqttSource.class);

    private MqttClient mqttClient;
    private MqttConnectOptions mqttConnectOptions;

    private Type type;
    private Gson gson;

    private String serverURI;
    private String topicName;
    private String userName;
    private String password;


    @Override
    public void configure(Context context) {
        String serverURI = context.getString("serverURI");
        String topicName = context.getString("topicName");
        String userName = context.getString("userName");
        String password = context.getString("password");

        this.serverURI = serverURI;
        this.topicName = topicName;
        this.userName = userName;
        this.password = password;
    }

    @Override
    public void start() {
        try {
            mqttConnectOptions.setCleanSession(true);
            mqttConnectOptions.setUserName(this.userName);
            mqttConnectOptions.setPassword(this.password.toCharArray());

            mqttClient = new MqttClient(serverURI, "Flume_clientId_" + InetAddress.getLocalHost().getCanonicalHostName()
                    + "_threadId_" + Thread.currentThread().getId());

            // Set this wrapper as the callback handler
            mqttClient.setCallback(this);

            // Connect to the MQTT server
            mqttClient.connect(mqttConnectOptions);

            LOG.info("Subscribing to topic: \"" + topicName + "\", QoS: " + 2);
            mqttClient.subscribe(topicName, 2);

            type = new TypeToken<Map<String, String>>() {
            }.getType();
            gson = new GsonBuilder().disableHtmlEscaping().create();

        } catch (MqttException e) {
            LOG.error("Error connecting to the MQTT broker.", e);
        } catch (UnknownHostException e) {
            LOG.error("Please define a hostname to be used as clientId.", e);
        }
    }

    @Override
    public void stop() {
        try {
            mqttClient.disconnect();
            mqttClient.close();
        } catch (MqttException e) {
            LOG.error("Error closing the connection to the MQTT broker.", e);
        }
    }

    @Override
    public void connectionLost(Throwable throwable) {
        LOG.info("Connection to " + serverURI + " lost!" + "\n" + throwable.getMessage());

        LOG.info("Trying to reconnect...");
        try {
            IMqttToken iMqttToken = mqttClient.connectWithResult(mqttConnectOptions);
            iMqttToken.waitForCompletion();
        } catch (MqttException e) {
            LOG.error("Reconnection failed.", e);
        }
    }

    @Override
    public void messageArrived(String s, MqttMessage mqttMessage) {
        // Called when a message arrives from the server that matches any
        // subscription made by the client
        LOG.trace("Time:\t" + System.currentTimeMillis() +
                "  Topic:\t" + topicName +
                "  Message:\t" + new String(mqttMessage.getPayload()) +
                "  QoS:\t" + mqttMessage.getQos());

        Map<String, String> eventMap = new HashMap<String, String>();
        try {
            eventMap = gson.fromJson(Arrays.toString(mqttMessage.getPayload()), type);
        } catch (JsonSyntaxException e) {
            LOG.error("Incoming mqttMessage payload has invalid JSON Syntax.", e);
        }

        final Event event = EventBuilder.withBody(null);
        event.setHeaders(eventMap);

        // Store the Event into this Source's associated Channel(s)
        getChannelProcessor().processEvent(event);
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {
    }
}
