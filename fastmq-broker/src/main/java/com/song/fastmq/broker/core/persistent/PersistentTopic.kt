package com.song.fastmq.broker.core.persistent

import com.google.common.base.Preconditions.checkArgument
import com.song.fastmq.broker.core.Consumer
import com.song.fastmq.broker.core.Producer
import com.song.fastmq.broker.core.ServerCnx
import com.song.fastmq.broker.core.Topic
import com.song.fastmq.broker.exception.TopicClosedException
import com.song.fastmq.broker.exception.TopicIllegalStateException
import com.song.fastmq.common.logging.Logger
import com.song.fastmq.common.logging.LoggerFactory
import com.song.fastmq.common.message.Message
import com.song.fastmq.common.message.MessageId
import com.song.fastmq.common.utils.OnCompletedObserver
import com.song.fastmq.storage.storage.MessageStorage
import com.song.fastmq.storage.storage.Offset
import io.netty.buffer.ByteBuf
import io.reactivex.Observable
import io.reactivex.ObservableEmitter
import org.apache.bookkeeper.util.collections.ConcurrentOpenHashSet
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * @author song
 */
class PersistentTopic(private val topic: String, private val messageStorage: MessageStorage) : Topic {

    override fun getTopic() = topic

    private val producers = ConcurrentOpenHashSet<Producer>()

    @Volatile
    private var isClosed = false

    private val lock = ReentrantLock()

    override fun subscribe(serverCnx: ServerCnx): Observable<Consumer> {
        return Observable.create<Consumer> { observable: ObservableEmitter<Consumer> ->
            observable.onNext(Consumer(serverCnx, this.messageStorage))
            observable.onComplete()
            return@create
        }
    }

    override fun addProducer(producer: Producer) {
        checkArgument(producer.topic == this)
        lock.withLock {
            if (isClosed) {
                logger.warn("[{}] Attempting to add producer to a closed topic", topic)
                throw TopicClosedException("Topic is already closed!")
            }

            if (!producers.add(producer)) {
                throw TopicIllegalStateException("Producer with name '${producer.producerName}' is already connected to topic")
            }
        }
    }

    override fun removeProducer(producer: Producer) {
        checkArgument(producer.topic == this)
        if (producers.remove(producer)) {
            logger.info("Remove producer[] from topic[{}].", producer.producerName, producer.topic.getTopic())
        }
    }

    override fun publishMessage(headersAndPayload: ByteBuf): Observable<Offset> {
        return Observable.create<Offset> { observable: ObservableEmitter<Offset> ->
            messageStorage.appendMessage(Message(MessageId.EMPTY, headersAndPayload.array()))
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
        this.lock.withLock {
            if (!isClosed) {
                messageStorage.close()
                isClosed = true
            }
        }
    }

    override fun toString(): String {
        return "PersistentTopic(topic='$topic')"
    }

    companion object {
        val logger: Logger = LoggerFactory.getLogger(PersistentTopic::class.java)
    }
}
