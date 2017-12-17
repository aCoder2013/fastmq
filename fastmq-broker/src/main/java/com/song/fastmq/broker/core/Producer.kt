package com.song.fastmq.broker.core

import io.netty.buffer.ByteBuf
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture


/**
 * @author song
 */
class Producer(val topic: Topic, val cnx: ServerCnxClient, val producerName: String, val producerId: Long) {

    @Volatile
    private var isClosed = false

    private var closeFuture: CompletableFuture<Void>? = null

    fun publishMessage(producerId: Long, sequenceId: Long, payload: ByteBuf) {
        topic.publishMessage(payload, object : Topic.PublishCallback {

            override fun onCompleted(ledgerId: Long, entryId: Long) {

            }

            override fun onThrowable(e: Throwable) {

            }
        })
    }

    override fun toString(): String {
        return "Producer(topic=$topic, producerName='$producerName', producerId=$producerId)"
    }

    companion object {
        private val logger = LoggerFactory.getLogger(Producer::class.java)
    }

}
