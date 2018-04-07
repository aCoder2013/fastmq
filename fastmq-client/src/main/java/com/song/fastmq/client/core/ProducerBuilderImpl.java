package com.song.fastmq.client.core;

import com.song.fastmq.client.core.conf.ProducerConfiguration;
import com.song.fastmq.client.exception.FastMqClientException;
import com.song.fastmq.client.producer.DefaultProducer;
import com.song.fastmq.client.producer.Producer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author song
 */
public class ProducerBuilderImpl implements ProducerBuilder {

    private final MQClientImpl mqClient;

    private final ProducerConfiguration configuration;

    public ProducerBuilderImpl(MQClientImpl client, ProducerConfiguration configuration) {
        this.mqClient = client;
        this.configuration = configuration;
    }

    @Override public Producer create() throws FastMqClientException {
        return null;
    }

    @Override public CompletableFuture<Producer> createAsync() {
        CompletableFuture<Producer> future = new CompletableFuture<>();
        Producer producer = new DefaultProducer(mqClient.getConnectionPool(), configuration.getTopicName(), configuration.getProducerName());
        try {
            producer.start();
            future.complete(producer);
        } catch (FastMqClientException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override public ProducerBuilder topic(String topic) {
        configuration.setTopicName(topic);
        return this;
    }

    @Override public ProducerBuilder producerName(String producerName) {
        configuration.setProducerName(producerName);
        return this;
    }

    @Override public ProducerBuilder sendTimeout(long sendTimeout, TimeUnit unit) {
        configuration.setSendTimeoutMs(unit.toMillis(sendTimeout));
        return this;
    }
}
