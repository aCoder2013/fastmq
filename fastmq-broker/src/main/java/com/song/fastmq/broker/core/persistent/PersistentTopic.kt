package com.song.fastmq.broker.core.persistent

import com.song.fastmq.broker.core.Topic
import com.song.fastmq.storage.storage.LogManager
import io.netty.buffer.ByteBuf
import java.util.concurrent.CompletableFuture

/**
 * @author song
 */
class PersistentTopic(topic: String, logManager: LogManager) : Topic {

    override fun getName(): String {
        return ""
    }

    override fun close(): CompletableFuture<Void> {
        return CompletableFuture()
    }

    override fun publishMessage(headersAndPayload: ByteBuf, callback: Topic.PublishCallback) {
        callback.onCompleted(0L, 0L)
    }

}
