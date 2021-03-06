package com.song.fastmq.broker.core

import com.google.common.collect.Lists
import com.song.fastmq.common.domain.MessageConstants
import com.song.fastmq.common.logging.LoggerFactory
import com.song.fastmq.common.utils.OnCompletedObserver
import com.song.fastmq.net.proto.BrokerApi
import com.song.fastmq.storage.storage.BatchMessage
import com.song.fastmq.storage.storage.MessageStorage
import com.song.fastmq.storage.storage.Offset
import io.netty.buffer.Unpooled
import java.util.*

/**
 * @author song
 */
class Consumer(private val cnx: ServerCnx, private val messageStorage: MessageStorage) {

    /**
     * todo:return Observable
     */
    fun readMessage(consumerId: Long, offset: Offset, maxToRead: Int) {
        val builder = BrokerApi.CommandMessage.newBuilder()
        builder.consumerId = consumerId
        val messages = Lists.newArrayListWithExpectedSize<BrokerApi.CommandSend>(maxToRead)
        messageStorage.queryMessage(offset, maxToRead)
                .blockingSubscribe(object : OnCompletedObserver<BatchMessage>() {
                    override fun onError(e: Throwable) {
                        logger.error("Read message failed_" + e.message, e)
                    }

                    override fun onNext(t: BatchMessage) {
                        t.messages.forEach {
                            val id = BrokerApi.MessageIdData.newBuilder()
                                    .setLedgerId(it.messageId.ledgerId)
                                    .setEntryId(it.messageId.entryId)
                                    .build()
                            val message = BrokerApi.CommandSend
                                    .newBuilder()
                                    .mergeFrom(it.data)
                                    .putHeaders(MessageConstants.MESSAGE_ID, Base64.getEncoder().encodeToString(id.toByteArray()))
                                    .build()
                            messages.add(message)
                        }
                        builder.nextReadOffset = BrokerApi.MessageIdData
                                .newBuilder()
                                .setLedgerId(t.nextReadOffset.ledgerId)
                                .setEntryId(t.nextReadOffset.entryId)
                                .build()
                        builder.addAllMessages(messages)
                    }

                    override fun onComplete() {
                        val command = BrokerApi.Command.newBuilder()
                                .setType(BrokerApi.Command.Type.MESSAGE)
                                .setMessage(builder.build())
                                .build()
                        cnx.ctx.channel()?.writeAndFlush(Unpooled.wrappedBuffer(command.toByteArray()))
                    }
                })
    }

    companion object {
        private val logger = LoggerFactory.getLogger(Consumer::class.java)
    }
}
