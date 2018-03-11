package com.song.fastmq.storage.storage

import com.song.fastmq.common.message.Message
import com.song.fastmq.storage.storage.support.LedgerStorageException
import io.reactivex.Observable

/**
 * Created by song on 2017/11/5.
 */
interface MessageStorage {

    fun appendMessage(message: Message): Observable<Offset>

    fun queryMessage(offset: Offset, maxMsgNum: Int): Observable<GetMessageResult>

    fun getNumberOfMessages() :Long

    @Throws(InterruptedException::class, LedgerStorageException::class)
    fun close()

}