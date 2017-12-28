package com.song.fastmq.broker.core

import com.google.common.collect.Lists
import com.song.fastmq.net.proto.BrokerApi
import com.song.fastmq.storage.common.domain.FastMQConfigKeys
import com.song.fastmq.storage.storage.LogReader
import java.util.*

/**
 * @author song
 */
class Consumer(private val logReader: LogReader) {

    fun readMessage(maxToRead: Int): List<BrokerApi.CommandSend> {
        val messages = Lists.newArrayListWithExpectedSize<BrokerApi.CommandSend>(maxToRead)
        val entries = logReader.readEntries(maxToRead)
        entries.forEach({
            val builder = BrokerApi.CommandSend.newBuilder().mergeFrom(it.data)
            val id = BrokerApi.MessageIdData.newBuilder()
                    .setLedgerId(it.offset.ledgerId)
                    .setEntryId(it.offset.entryId)
                    .build()
            builder.putHeaders(FastMQConfigKeys.MESSAGE_ID, Base64.getEncoder().encodeToString(id.toByteArray()))
            messages.add(builder.build())
        })
        return messages
    }
}
