package com.song.fastmq.broker.core

import com.google.common.collect.Lists
import com.song.fastmq.net.proto.BrokerApi
import com.song.fastmq.storage.common.domain.FastMQConfigKeys
import com.song.fastmq.storage.common.utils.OnCompletedObserver
import com.song.fastmq.storage.storage.GetMessageResult
import com.song.fastmq.storage.storage.MessageStorage
import com.song.fastmq.storage.storage.Offset
import io.reactivex.schedulers.Schedulers
import org.slf4j.LoggerFactory
import java.util.*

/**
 * @author song
 */
class Consumer(private val messageStorage: MessageStorage) {

    fun readMessage(offset: Offset, maxToRead: Int): BrokerApi.Command {
        val builder = BrokerApi.CommandMessage.newBuilder()
        builder.consumerId = -1
        val messages = Lists.newArrayListWithExpectedSize<BrokerApi.CommandSend>(maxToRead)
        messageStorage.queryMessage(offset, maxToRead)
                .subscribeOn(Schedulers.io())
                .subscribe(object : OnCompletedObserver<GetMessageResult>() {
                    override fun onError(e: Throwable) {
                        logger.error("Read message failed_" + e.message, e)
                    }

                    override fun onNext(t: GetMessageResult) {
                        t.messages.forEach {
                            val id = BrokerApi.MessageIdData.newBuilder()
                                    .setLedgerId(it.messageId.ledgerId)
                                    .setEntryId(it.messageId.entryId)
                                    .build()
                            val message = BrokerApi.CommandSend
                                    .newBuilder()
                                    .mergeFrom(it.data)
                                    .putHeaders(FastMQConfigKeys.MESSAGE_ID, Base64.getEncoder().encodeToString(id.toByteArray()))
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
                    }

                })

        return BrokerApi.Command.newBuilder()
                .setType(BrokerApi.Command.Type.MESSAGE)
                .setMessage(builder)
                .build()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(Consumer::class.java)
    }
}
