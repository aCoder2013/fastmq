package com.song.fastmq.client.core;

import com.song.fastmq.client.exception.FastMqClientException;
import java.util.concurrent.TimeUnit;

/**
 * Builder that's used to construct a {@link MQClient} instance.
 *
 * @author song
 */
public interface ClientBuilder {

    /**
     * @return a new {@link MQClient} instance
     * @throws FastMqClientException
     */
    MQClient builder() throws FastMqClientException;

    /**
     * configure the broker server list,the parameter is required
     *
     * @param servers the broker server list
     * @return
     */
    ClientBuilder brokerServers(String servers);

    /**
     * Configure the operation timeout,default :30 seconds
     * <p>
     * Producer-create, subscribe and unsubscribe operations will be retried until this interval, after which the
     * operation will be marked as failed
     * </p>
     *
     * @param timeout operation timeout
     * @param unit time unit for timeout
     * @return
     */
    ClientBuilder operationTimeout(long timeout, TimeUnit unit);

    /**
     * @param num the number of threads that's used to handle io events.
     * @return
     */
    ClientBuilder ioThreads(int num);

    /**
     * Configure whether to use TCP no-delay flag on the connection, to disable Nagle algorithm.
     *
     * @param useTcpNoDelay default is true
     * @return
     */
    ClientBuilder enableTcpNoDelay(boolean useTcpNoDelay);
}
