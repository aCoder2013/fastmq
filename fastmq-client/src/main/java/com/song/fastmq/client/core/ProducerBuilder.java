package com.song.fastmq.client.core;

import com.song.fastmq.client.exception.FastMqClientException;
import com.song.fastmq.client.producer.Producer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author song
 */
public interface ProducerBuilder {

    Producer create() throws FastMqClientException;

    CompletableFuture<Producer> createAsync();

    /**
     * Specify the topic this producer will be publishing on.
     * <p>
     * This argument is required when constructing the produce.
     * </p>
     *
     * @param topic
     * @return
     */
    ProducerBuilder topic(String topic);

    /**
     * Specify a name for the producer
     * <p>
     * If not assigned, the system will generate a globally unique name which can be access with
     * {@link Producer#getProducerName()}.
     * <p>
     * When specifying a name, it is up to the user to ensure that, for a given topic, the producer name is unique
     * across all FastMQ's clusters. Brokers will enforce that only a single producer a given name can be publishing on
     * a topic.
     *
     * @param producerName
     * @return
     */
    ProducerBuilder producerName(String producerName);

    /**
     * Set the send timeout <i>(default: 30 seconds)</i>
     * <p>
     * If a message is not acknowledged by the server before the sendTimeout expires, an error will be reported.
     *
     * @param sendTimeout the send timeout
     * @param unit the time unit of the {@code sendTimeout}
     */
    ProducerBuilder sendTimeout(long sendTimeout, TimeUnit unit);
}
