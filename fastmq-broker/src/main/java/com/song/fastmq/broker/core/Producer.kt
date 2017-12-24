package com.song.fastmq.broker.core

import com.song.fastmq.net.proto.Commands
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture


/**
 * @author song
 */
class Producer(private val topic: Topic, val cnx: ServerCnx, private val producerName: String, private val producerId: Long) {

    @Volatile
    private var isClosed = false

    private var closeFuture: CompletableFuture<Void>? = null

    fun publishMessage(producerId: Long, sequenceId: Long, payload: ByteBuf) {
        topic.publishMessage(payload, object : Topic.PublishCallback {

            override fun onCompleted(ledgerId: Long, entryId: Long) {
                val sendReceipt = Commands.newSendReceipt(producerId, sequenceId, ledgerId, entryId)
                cnx.ctx?.writeAndFlush(Unpooled.wrappedBuffer(sendReceipt.toByteArray()))
            }

            override fun onThrowable(e: Throwable) {
                val sendError = Commands.newSendError(producerId, sequenceId, e)
                cnx.ctx?.writeAndFlush(Unpooled.wrappedBuffer(sendError.toByteArray()))
            }
        })
    }

    fun close() {
        topic.close()
    }

    override fun toString(): String {
        return "Producer(topic=$topic, producerName='$producerName', producerId=$producerId)"
    }

    companion object {
        private val logger = LoggerFactory.getLogger(Producer::class.java)
    }

}
