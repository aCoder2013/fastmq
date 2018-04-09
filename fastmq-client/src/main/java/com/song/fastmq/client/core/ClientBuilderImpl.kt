package com.song.fastmq.client.core

import com.song.fastmq.client.core.conf.ClientConfiguration
import com.song.fastmq.client.exception.FastMqClientException
import java.util.concurrent.TimeUnit

/**
 * @author song
 */
class ClientBuilderImpl @JvmOverloads constructor(private val configuration: ClientConfiguration = ClientConfiguration()) :
    ClientBuilder {

    @Throws(FastMqClientException::class)
    override fun builder(): MQClient {
        if (configuration.brokerServers.isNullOrBlank()) {
            throw IllegalArgumentException("Broker servers can't be null or empty!")
        }
        return MQClientImpl(configuration)
    }

    override fun brokerServers(servers: String): ClientBuilder {
        configuration.brokerServers = servers
        return this
    }

    override fun operationTimeout(timeout: Long, unit: TimeUnit): ClientBuilder {
        configuration.operationTimeoutMs = unit.toMillis(timeout)
        return this
    }

    override fun ioThreads(num: Int): ClientBuilder {
        configuration.numOfIoThreads = num
        return this
    }

    override fun enableTcpNoDelay(useTcpNoDelay: Boolean): ClientBuilder {
        configuration.useTcpNoDelay = useTcpNoDelay
        return this
    }

}
