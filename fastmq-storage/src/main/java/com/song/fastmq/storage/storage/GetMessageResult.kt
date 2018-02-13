package com.song.fastmq.storage.storage

import com.song.fastmq.storage.common.message.Message

/**
 * @author song
 */
class GetMessageResult(var nextReadOffset: Offset = Offset.NULL_OFFSET, val messages: List<Message>)
