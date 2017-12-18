package com.song.fastmq.client.impl

import com.song.fastmq.client.BytesMessageImpl
import io.openmessaging.*
import java.util.concurrent.atomic.AtomicReference

/**
 * @author song
 */
class DefaultProducer(private val properties: KeyValue) : Producer {

    private var state = AtomicReference<State>(State.NONE)

    private var clientCnx: ClientCnxClient? = null

    private val cnxPool: RemotingConnectionPool = RemotingConnectionPool()

    override fun startup() {
        state.compareAndSet(State.NONE, State.CONNECTED)
    }

    override fun sendOneway(message: Message) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun sendOneway(message: Message, properties: KeyValue) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun createBytesMessageToQueue(queue: String, body: ByteArray): BytesMessage {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun properties(): KeyValue {
        return this.properties
    }

    override fun sendAsync(message: Message): Promise<SendResult> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun sendAsync(message: Message, properties: KeyValue): Promise<SendResult> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun send(message: Message): SendResult {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun send(message: Message, properties: KeyValue?): SendResult {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun createBytesMessageToTopic(topic: String, body: ByteArray): BytesMessage {
        val bytesMessage = BytesMessageImpl()
        bytesMessage.setBody(body)
        bytesMessage.headers().put(MessageHeader.TOPIC, topic)
        return bytesMessage
    }

    override fun shutdown() {
        state.set(State.NONE)
    }

    enum class State {
        NONE,
        CONNECTING,
        CONNECTED
    }
}