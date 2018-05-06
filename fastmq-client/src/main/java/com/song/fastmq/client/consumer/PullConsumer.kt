package com.song.fastmq.client.consumer

import com.song.fastmq.client.domain.Message
import com.song.fastmq.client.domain.MessageId
import java.util.concurrent.TimeUnit


/**
 * @author song
 */
interface PullConsumer {

    fun start()

    fun shutdown()

    /**
     * Pull the messages with nonblocking way
     */
    fun poll(): Message

    /**
     * Pull message async way
     */
    @Throws(InterruptedException::class)
    fun poll(timeout: Long, unit: TimeUnit): Message

    /**
     * Update the offset
     */
    fun updateConsumeOffset(messageId: MessageId)
}
