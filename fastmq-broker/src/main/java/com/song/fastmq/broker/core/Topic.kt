package com.song.fastmq.broker.core

import io.netty.buffer.ByteBuf
import java.util.concurrent.CompletableFuture

/**
 * @author song
 */
interface Topic {

    interface PublishCallback {

        fun onCompleted(ledgerId: Long, entryId: Long)

        fun onThrowable(e: Throwable)
    }

    fun publishMessage(headersAndPayload: ByteBuf, callback: PublishCallback)

    fun getName(): String

    fun close()
}