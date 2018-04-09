package com.song.fastmq.client.domain;

import java.util.HashMap;
import java.util.Map;

/**
 * @author song
 */
public class Message {

    private byte[] body;

    private Map<String, String> properties = new HashMap<>();

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }
}
