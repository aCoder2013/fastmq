package com.song.fastmq.broker.core

import com.song.fastmq.net.proto.Commands
import com.song.fastmq.storage.storage.Offset
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.reactivex.Observer
import io.reactivex.disposables.Disposable
import io.reactivex.schedulers.Schedulers
import org.slf4j.LoggerFactory


/**
 * @author song
 */
class Producer(private val topic: Topic, val cnx: ServerCnx, private val producerName: String, private val producerId: Long) {

    @Volatile
    private var isClosed = false

    fun publishMessage(producerId: Long, sequenceId: Long, payload: ByteBuf) {
        this.topic.publishMessage(payload)
                .subscribeOn(Schedulers.io())
                .subscribe(object : Observer<Offset> {

                    override fun onNext(t: Offset) {
                        val sendReceipt = Commands.newSendReceipt(producerId, sequenceId, t.ledgerId, t.entryId)
                        cnx.ctx?.writeAndFlush(Unpooled.wrappedBuffer(sendReceipt.toByteArray()))
                    }

                    override fun onError(e: Throwable) {
                        val sendError = Commands.newSendError(producerId, sequenceId, e)
                        cnx.ctx?.writeAndFlush(Unpooled.wrappedBuffer(sendError.toByteArray()))
                    }

                    override fun onSubscribe(d: Disposable) {
                    }

                    override fun onComplete() {
                        logger.debug("Producer[$producerId] publish message[$sequenceId] done.")
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
