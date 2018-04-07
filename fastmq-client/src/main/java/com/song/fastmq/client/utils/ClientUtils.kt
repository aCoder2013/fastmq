package com.song.fastmq.client.utils

import com.google.protobuf.ByteString
import com.song.fastmq.client.domain.Message
import com.song.fastmq.net.proto.BrokerApi
import java.util.concurrent.atomic.AtomicLong
import java.util.function.Consumer

/**
 * @author song
 */
object ClientUtils {

    private val producerIdGenerator = AtomicLong(System.currentTimeMillis())

    private val consumerIdGenerator = AtomicLong(System.currentTimeMillis())

    private val requestIdGenerator = AtomicLong()

    private const val SEPARATOR = ";"

    fun getNextRequestId(): Long {
        return requestIdGenerator.incrementAndGet()
    }

    fun getNextProducerId(): Long {
        return producerIdGenerator.incrementAndGet()
    }

    fun getNextConsumerId(): Long {
        return consumerIdGenerator.incrementAndGet()
    }

    fun parseBootstrapServers(accessPoints: String): ArrayList<String> {
        val bootstrapServers = ArrayList<String>()
        accessPoints.split(SEPARATOR).forEach(Consumer {
            bootstrapServers += it
        })
        check(bootstrapServers.isNotEmpty())
        return bootstrapServers
    }

    /**
     * Convert a message into protocol-buffers message
     */
    fun msgConvert(message: Message): BrokerApi.CommandSend {
        val builder = BrokerApi.CommandSend.newBuilder()
        message.properties?.forEach({ k, v ->
            builder.putHeaders(k, v)
        })
        builder.body = ByteString.copyFrom(message.body)
        return builder.build()
    }
}