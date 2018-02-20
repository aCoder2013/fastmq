package com.song.fastmq.storage.common.message

/**
 * @author song
 */
data class MessageId(val ledgerId: Long, val entryId: Long) {

    companion object {
        val EMPTY = MessageId(0L, 0L)
    }
}