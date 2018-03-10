package com.song.fastmq.storage.storage

import io.reactivex.Observable

/**
 * A factory to manage ledgers.
 *
 * Created by song on 2017/11/4.
 */
interface MessageStorageFactory {

    /**
     * Open a message storage of given topic. If it doesn't exist, a new one will be automatically
     * created.
     *
     * @param name name of the topic,must be unique.
     * @return the message storage
     */
    fun open(name: String): Observable<MessageStorage>

    fun close(topic: String)

    fun close()
}

