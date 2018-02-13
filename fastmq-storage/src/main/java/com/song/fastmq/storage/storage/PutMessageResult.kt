package com.song.fastmq.storage.storage

import com.song.fastmq.storage.common.message.MessageId

/**
 * @author song
 */
class PutMessageResult(private val status: StatusEnum, private val msgId: MessageId = MessageId.EMPTY, private val storeTimestamp: Long = 0) {

    enum class StatusEnum {
        OK, UNKNOWN_ERROR
    }
}