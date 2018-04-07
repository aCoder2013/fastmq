package com.song.fastmq.client.producer

import com.song.fastmq.client.domain.Message
import com.song.fastmq.client.domain.MessageId
import com.song.fastmq.client.exception.FastMqClientException
import java.util.concurrent.CompletableFuture

/**
 * @author song
 */
interface Producer {

    @Throws(FastMqClientException::class)
    fun start()

    fun shutdown()

    @Throws(FastMqClientException::class)
    fun send(message: Message): MessageId?

    fun sendAsync(message: Message): CompletableFuture<MessageId>

    /**
     * @return the producer name which could have been assigned by the system or specified by the client
     */
    fun getProducerName(): String
}
