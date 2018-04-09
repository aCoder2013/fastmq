package com.song.fastmq.common.message

/**
 * @author song
 */
class Message(
    var messageId: MessageId = MessageId.EMPTY,
    val data: ByteArray
) {


}