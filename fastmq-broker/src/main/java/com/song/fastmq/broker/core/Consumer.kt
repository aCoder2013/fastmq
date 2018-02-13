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

    fun readMessage(offset: Offset, maxToRead: Int): List<BrokerApi.CommandSend> {
        val messages = Lists.newArrayListWithExpectedSize<BrokerApi.CommandSend>(maxToRead)
        messageStorage.queryMessage(offset, maxToRead)
                .subscribeOn(Schedulers.io())
                .subscribe(object : OnCompletedObserver<GetMessageResult>() {
                    override fun onError(e: Throwable) {
                        logger.error("Read message failed_" + e.message, e)
                    }

                    override fun onNext(t: GetMessageResult) {
                        t.messages.forEach {
                            val builder = BrokerApi.CommandSend.newBuilder().mergeFrom(it.data)
                            val id = BrokerApi.MessageIdData.newBuilder()
                                    .setLedgerId(it.messageId.ledgerId)
                                    .setEntryId(it.messageId.entryId)
                                    .build()
                            builder.putHeaders(FastMQConfigKeys.MESSAGE_ID, Base64.getEncoder().encodeToString(id.toByteArray()))
                            messages.add(builder.build())
                        }
                    }

                    override fun onComplete() {
                    }

                })
        return messages
    }

    companion object {
        private val logger = LoggerFactory.getLogger(Consumer::class.java)
    }
}
