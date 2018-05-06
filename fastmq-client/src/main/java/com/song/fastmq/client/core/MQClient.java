package com.song.fastmq.client.core;

import com.song.fastmq.client.consumer.PullConsumer;
import java.io.Closeable;

/**
 * @author song
 */
public interface MQClient extends Closeable {

    static ClientBuilder builder() {
        return new ClientBuilderImpl();
    }

    ProducerBuilder createProducer();

    PullConsumer createPullConsumer();
}
