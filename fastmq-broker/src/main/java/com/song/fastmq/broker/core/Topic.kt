package com.song.fastmq.broker.core

import com.song.fastmq.storage.storage.Offset
import io.netty.buffer.ByteBuf
import io.reactivex.Observable

/**
 * @author song
 */
interface Topic {

    fun publishMessage(headersAndPayload: ByteBuf) :Observable<Offset>

    fun getName(): String

    fun close()
}