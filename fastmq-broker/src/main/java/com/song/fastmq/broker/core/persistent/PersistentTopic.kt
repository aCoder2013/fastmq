package com.song.fastmq.broker.core.persistent

import com.song.fastmq.broker.core.Topic
import com.song.fastmq.storage.common.message.Message
import com.song.fastmq.storage.storage.MessageStorage
import com.song.fastmq.storage.storage.Offset
import io.netty.buffer.ByteBuf
import io.reactivex.Observable
import io.reactivex.ObservableEmitter

/**
 * @author song
 */
class PersistentTopic(private val topic: String, private val messageStorage: MessageStorage) : Topic {

    override fun getName() = topic

    override fun publishMessage(headersAndPayload: ByteBuf): Observable<Offset> {
        return Observable.create<Offset> { observable: ObservableEmitter<Offset> ->
            messageStorage.appendMessage(Message(data = headersAndPayload.array()))
                    .doOnNext {
                        observable.onNext(it)
                        headersAndPayload.release()
                    }
                    .doOnComplete {
                        observable.onComplete()
                    }
                    .doOnError {
                        headersAndPayload.release()
                        observable.onError(it)
                    }
        }
    }

    override fun close() {
        messageStorage.close()
    }

}
