package com.song.fastmq.broker.core.persistent

import com.song.fastmq.broker.core.Topic
import com.song.fastmq.storage.storage.LogManager
import com.song.fastmq.storage.storage.Offset
import com.song.fastmq.storage.storage.Version
import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks
import com.song.fastmq.storage.storage.support.LedgerStorageException
import io.netty.buffer.ByteBuf

/**
 * @author song
 */
class PersistentTopic(val topic: String, val logManager: LogManager) : Topic {

    override fun getName(): String {
        return ""
    }

    override fun close() {
        logManager.close()
    }

    override fun publishMessage(headersAndPayload: ByteBuf, callback: Topic.PublishCallback) {
        logManager.asyncAddEntry(headersAndPayload.array(), object : AsyncCallbacks.CommonCallback<Offset, LedgerStorageException> {
            override fun onCompleted(data: Offset?, version: Version?) {
                headersAndPayload.release()
                data?.let {
                    callback.onCompleted(it.ledgerId, it.entryId)
                } ?: callback.onThrowable(LedgerStorageException("Offset is absent,this shouldn't happen."))
            }

            override fun onThrowable(throwable: LedgerStorageException) {
                headersAndPayload.release()
                callback.onThrowable(throwable)
            }
        })
    }

}
