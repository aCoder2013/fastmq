package com.song.fastmq.client.core;

import com.song.fastmq.client.consumer.PullConsumer;
import com.song.fastmq.client.producer.Producer;
import java.io.Closeable;

/**
 * @author song
 */
public interface MQClient extends Closeable {

    static ClientBuilder builder() {
        return new ClientBuilderImpl();
    }

    Producer createProducer();

    PullConsumer createPullConsumer();
}
