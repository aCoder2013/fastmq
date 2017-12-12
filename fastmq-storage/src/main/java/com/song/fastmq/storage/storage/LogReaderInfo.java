package com.song.fastmq.storage.storage;

/**
 * @author song
 */
public class LogReaderInfo {

    private String topic;

    private String consumer;

    public LogReaderInfo() {
    }

    public LogReaderInfo(String topic, String consumer) {
        this.topic = topic;
        this.consumer = consumer;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getConsumer() {
        return consumer;
    }

    public void setConsumer(String consumer) {
        this.consumer = consumer;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LogReaderInfo that = (LogReaderInfo) o;

        if (!topic.equals(that.topic)) {
            return false;
        }
        return consumer.equals(that.consumer);
    }

    @Override
    public int hashCode() {
        int result = topic.hashCode();
        result = 31 * result + consumer.hashCode();
        return result;
    }
}
