package com.song.fastmq.client.consumer

import com.song.fastmq.client.domain.Message
import com.song.fastmq.client.domain.MessageId


/**
 * @author song
 */
interface PullConsumer {

    fun start()

    fun shutdown()

    /**
     * Pull the messages with nonblocking way
     */
    fun poll(): List<Message>

    /**
     * Pull message async way
     */
    fun poll(pullCallback: PullCallback)

    /**
     * Update the offset
     */
    fun updateConsumeOffset(messageId: MessageId)
}
