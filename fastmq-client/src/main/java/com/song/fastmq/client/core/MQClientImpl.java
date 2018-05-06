package com.song.fastmq.client.core;

import com.google.common.collect.Maps;
import com.song.fastmq.client.consumer.PullConsumer;
import com.song.fastmq.client.core.conf.ClientConfiguration;
import com.song.fastmq.client.core.conf.ProducerConfiguration;
import com.song.fastmq.client.exception.FastMqClientException;
import com.song.fastmq.client.net.RemotingConnectionPool;
import com.song.fastmq.client.producer.DefaultProducer;
import com.song.fastmq.client.producer.Producer;
import com.song.fastmq.net.utils.NettyUtils;
import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import java.io.IOException;
import java.util.IdentityHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author song
 */
public class MQClientImpl implements MQClient {

    private static final Logger logger = LoggerFactory.getLogger(MQClientImpl.class);

    private AtomicReference<State> state = new AtomicReference<>();

    private ClientConfiguration configuration;

    private EventLoopGroup eventLoopGroup;

    private RemotingConnectionPool connectionPool;

    private final IdentityHashMap<Producer, Boolean> producers;

    private final IdentityHashMap<PullConsumer, Boolean> consumers;

    private final AtomicLong producerIdGenerator = new AtomicLong();
    private final AtomicLong consumerIdGenerator = new AtomicLong();
    private final AtomicLong requestIdGenerator = new AtomicLong();

    public MQClientImpl(ClientConfiguration configuration) throws FastMqClientException {
        this(configuration, getEventLoopGroup(configuration));
    }

    public MQClientImpl(ClientConfiguration configuration,
        EventLoopGroup eventLoopGroup) throws FastMqClientException {
        this(configuration, eventLoopGroup, new RemotingConnectionPool(configuration, eventLoopGroup));
    }

    public MQClientImpl(ClientConfiguration configuration, EventLoopGroup eventLoopGroup,
        RemotingConnectionPool connectionPool) throws FastMqClientException {
        if (configuration == null || StringUtils.isBlank(configuration.getBrokerServers())
            || eventLoopGroup == null || connectionPool == null) {
            throw new FastMqClientException("Invalid configuration.");
        }
        this.configuration = configuration;
        this.eventLoopGroup = eventLoopGroup;
        this.connectionPool = connectionPool;

        this.producers = Maps.newIdentityHashMap();
        this.consumers = Maps.newIdentityHashMap();

        state.set(State.Open);
    }

    private static EventLoopGroup getEventLoopGroup(ClientConfiguration configuration) {
        ThreadFactory threadFactory = new DefaultThreadFactory("fastmq-client-io");
        return NettyUtils.INSTANCE.newEventLoopGroup(configuration.getNumOfIoThreads(), threadFactory);
    }

    @Override public ProducerBuilder createProducer() {
        // TODO: 2018/4/14  
        return null;
    }

    @Override public PullConsumer createPullConsumer() {
        return null;
    }

    public CompletableFuture<Producer> createProducerAsync(ProducerConfiguration configuration) {
        CompletableFuture<Producer> future = new CompletableFuture<>();
        if (configuration == null) {
            future.completeExceptionally(new FastMqClientException("Invalid producer configuration!"));
            return future;
        }

        if (state.get() != State.Open) {
            future.completeExceptionally(new FastMqClientException("MQClient is already closed, state = " + state.get()));
            return future;
        }

        if (StringUtils.isBlank(configuration.getTopicName())) {
            future.completeExceptionally(new FastMqClientException("Invalid topic name"));
            return future;
        }

        Producer producer = new DefaultProducer(connectionPool, configuration.getTopicName(), configuration.getProducerName());
        try {
            producer.start();
            synchronized (producers) {
                producers.put(producer, Boolean.TRUE);
            }
            future.complete(producer);
        } catch (FastMqClientException e) {
            future.completeExceptionally(e);
        }
        return future;
    }

    @Override public void close() throws IOException {

    }

    public RemotingConnectionPool getConnectionPool() {
        return this.connectionPool;
    }

    enum State {
        Open, Closing, Closed
    }

}
