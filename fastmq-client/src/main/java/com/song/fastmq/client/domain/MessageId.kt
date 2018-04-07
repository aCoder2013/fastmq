package com.song.fastmq.client.domain

import com.google.common.collect.ComparisonChain
import com.google.protobuf.UninitializedMessageException
import com.song.fastmq.net.proto.BrokerApi
import java.io.IOException

/**
 * @author song
 */
class MessageId(val ledgerId: Long, val entryId: Long) : Comparable<MessageId> {

    override operator fun compareTo(other: MessageId): Int {
        return ComparisonChain.start().compare(this.ledgerId, other.ledgerId).compare(this.entryId, other.entryId)
                .result()
    }

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as MessageId

        if (ledgerId != other.ledgerId) return false
        if (entryId != other.entryId) return false

        return true
    }

    override fun hashCode(): Int {
        var result = ledgerId.hashCode()
        result = 31 * result + entryId.hashCode()
        return result
    }

    override fun toString(): String {
        return "MessageId(ledgerId=$ledgerId, entryId=$entryId)"
    }

    fun toByteArray(): ByteArray {
        val builder = BrokerApi.MessageIdData.newBuilder()
        builder.ledgerId = ledgerId
        builder.entryId = entryId
        val msgId = builder.build()
        return msgId.toByteArray()
    }

    companion object {

        val NULL_ID = MessageId(-1, -1)

        @Throws(IOException::class)
        fun fromByteArray(data: ByteArray): MessageId {
            checkNotNull(data)
            val builder = BrokerApi.MessageIdData.newBuilder()

            val idData: BrokerApi.MessageIdData
            try {
                idData = builder.mergeFrom(data).build()
            } catch (e: UninitializedMessageException) {
                throw IOException(e)
            }
            return MessageId(idData.ledgerId, idData.entryId)
        }
    }
}