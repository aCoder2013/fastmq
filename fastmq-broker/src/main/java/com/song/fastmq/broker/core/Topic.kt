package com.song.fastmq.broker.core

import com.song.fastmq.storage.storage.Offset
import io.netty.buffer.ByteBuf
import io.reactivex.Observable

/**
 * @author song
 */
interface Topic {

    fun subscribe(serverCnx: ServerCnx): Observable<Consumer>

    fun publishMessage(headersAndPayload: ByteBuf): Observable<Offset>

    fun getTopic(): String

    fun addProducer(producer: Producer)

    fun removeProducer(producer: Producer)

    fun close()
}