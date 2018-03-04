package com.song.fastmq.broker.core.persistent

import com.song.fastmq.broker.core.Topic
import com.song.fastmq.common.message.Message
import com.song.fastmq.common.utils.OnCompletedObserver
import com.song.fastmq.storage.storage.MessageStorage
import com.song.fastmq.storage.storage.Offset
import io.netty.buffer.ByteBuf
import io.reactivex.Observable
import io.reactivex.ObservableEmitter
import org.slf4j.LoggerFactory

/**
 * @author song
 */
class PersistentTopic(private val topic: String, private val messageStorage: MessageStorage) : Topic {

    override fun getName() = topic

    override fun publishMessage(headersAndPayload: ByteBuf): Observable<Offset> {
        return Observable.create<Offset> { observable: ObservableEmitter<Offset> ->
            messageStorage.appendMessage(Message(data = headersAndPayload.array()))
                    .subscribe(object : OnCompletedObserver<Offset>() {

                        override fun onNext(t: Offset) {
                            headersAndPayload.release()
                            observable.onNext(t)
                            observable.onComplete()
                        }

                        override fun onError(e: Throwable) {
                            headersAndPayload.release()
                            observable.onError(e)
                        }
                    })
        }
    }

    override fun close() {
        messageStorage.close()
    }


    companion object {
        val logger = LoggerFactory.getLogger(PersistentTopic::class.java)
    }
}
