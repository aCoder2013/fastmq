package com.song.fastmq.storage.storage

import com.song.fastmq.common.message.Message

/**
 * @author song
 */
class BatchMessage(var nextReadOffset: Offset = Offset.NULL_OFFSET, val messages: List<Message>)
