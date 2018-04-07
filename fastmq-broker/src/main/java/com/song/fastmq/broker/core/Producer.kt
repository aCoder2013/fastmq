package com.song.fastmq.broker.core

import com.song.fastmq.net.proto.Commands
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import org.slf4j.LoggerFactory


/**
 * @author song
 */
class Producer(private val topic: Topic, val cnx: ServerCnx, private val producerName: String, private val producerId: Long) {

    @Volatile
    private var isClosed = false

    fun publishMessage(producerId: Long, sequenceId: Long, payload: ByteBuf) {
        this.topic.publishMessage(payload)
                .subscribe({
                    val sendReceipt = Commands.newSendReceipt(producerId, sequenceId, it.ledgerId, it.entryId)
                    cnx.ctx.writeAndFlush(Unpooled.wrappedBuffer(sendReceipt.toByteArray()))
                }, {
                    logger.error("[$topic] [$producerName] Publish message failed.", it)
                    val sendError = Commands.newSendError(producerId, sequenceId, it)
                    cnx.ctx.writeAndFlush(Unpooled.wrappedBuffer(sendError.toByteArray()))
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
