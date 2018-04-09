package com.song.fastmq.broker.core

import com.song.fastmq.broker.exception.MessagePublichException
import com.song.fastmq.common.logging.LoggerFactory
import com.song.fastmq.net.proto.Commands
import com.song.fastmq.storage.storage.Offset
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.reactivex.Observer
import io.reactivex.disposables.Disposable
import java.util.concurrent.atomic.AtomicBoolean


/**
 * @author song
 */
class Producer(val topic: Topic, val cnx: ServerCnx, val producerName: String, val producerId: Long) {

    @Volatile
    private var isClosed = AtomicBoolean(false)

    fun publishMessage(producerId: Long, sequenceId: Long, payload: ByteBuf) {
        if (isClosed()) {
            val sendError =
                Commands.newSendError(producerId, sequenceId, MessagePublichException("Producer is already closed!"))
            cnx.ctx.writeAndFlush(Unpooled.wrappedBuffer(sendError.toByteArray()))
            return
        }
        this.topic.publishMessage(payload)
            .subscribe(object : Observer<Offset> {

                override fun onNext(t: Offset) {
                    logger.debug("Successfully publish message with offset {}.", t)
                    val sendReceipt = Commands.newSendReceipt(producerId, sequenceId, t.ledgerId, t.entryId)
                    cnx.ctx.writeAndFlush(Unpooled.wrappedBuffer(sendReceipt.toByteArray()))
                }

                override fun onError(e: Throwable) {
                    logger.error("Publish message failed_${e.message}", e)
                    val sendError = Commands.newSendError(producerId, sequenceId, e)
                    cnx.ctx.writeAndFlush(Unpooled.wrappedBuffer(sendError.toByteArray()))
                }

                override fun onSubscribe(d: Disposable) {
                }

                override fun onComplete() {
                    logger.debug("Producer[$producerId] publish message[$sequenceId] done.")
                }
            })
    }

    /**
     * Check if producer is closed
     */
    fun isClosed(): Boolean = isClosed.get()

    @Synchronized
    fun close() {
        if (this.isClosed.compareAndSet(false, true)) {
            this.topic.removeProducer(this)
            this.cnx.removeProducer(this)
            isClosed.set(true)
        } else {
            logger.warn("Producer[{}] {}-{} is already closed.", this.topic, this.producerName, this.producerId)
        }
    }

    override fun toString(): String {
        return "Producer(topic=$topic, producerName='$producerName', producerId=$producerId)"
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as Producer

        if (topic.getTopic() != other.topic.getTopic()) return false
        if (producerName != other.producerName) return false

        return true
    }

    override fun hashCode(): Int {
        var result = topic.getTopic().hashCode()
        result = 31 * result + producerName.hashCode()
        return result
    }

    companion object {
        private val logger = LoggerFactory.getLogger(Producer::class.java)
    }

}
