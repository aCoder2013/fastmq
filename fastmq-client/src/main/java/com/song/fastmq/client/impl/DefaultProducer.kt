package com.song.fastmq.client.impl

import com.song.fastmq.client.BytesMessageImpl
import com.song.fastmq.client.ErrorCode
import com.song.fastmq.client.utils.ClientUtils
import com.song.fastmq.net.proto.BrokerApi
import com.song.fastmq.storage.common.utils.Utils
import io.netty.buffer.Unpooled
import io.openmessaging.*
import io.openmessaging.exception.OMSException
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Consumer

/**
 * @author song
 */
class DefaultProducer(private val properties: KeyValue) : Producer {

    private var name: String? = null

    private var topic: String? = null

    private val bootstrapServers = ArrayList<String>()

    private var state = AtomicReference<State>(State.NONE)

    private var clientCnx: ClientCnxClient? = null

    private val cnxPool: RemotingConnectionPool = RemotingConnectionPool()

    @Throws(Exception::class)
    override fun startup() {
        if (state.compareAndSet(State.NONE, State.CONNECTING)) {
            name = this.properties.getString(PropertyKeys.PRODUCER_ID)
            topic = this.properties.getString(PropertyKeys.SRC_TOPIC)
            if (name.isNullOrBlank()) {
                name = ClientUtils.buildInstanceName()
            }
            val accessPoints = this.properties.getString(PropertyKeys.ACCESS_POINTS)
            accessPoints.split(SEPARATOR).forEach(Consumer {
                bootstrapServers.add(it)
            })
            val connectionFuture = cnxPool.getConnection(Utils.string2SocketAddress(bootstrapServers.get(0)))
            this.clientCnx = connectionFuture.get()
            val producer = BrokerApi.CommandProducer.newBuilder().setProducerId(1L).setProducerName("Hello").setTopic(topic).setRequestId(1L)
                    .build()
            val command = BrokerApi.Command.newBuilder().setProducer(producer).setType(BrokerApi.Command.Type.PRODUCER).build()
            val toByteArray = command.toByteArray()
            val byteBuf = Unpooled.buffer(4 + toByteArray.size)
            val size = toByteArray.size
            byteBuf.writeInt(size)
            byteBuf.writeBytes(toByteArray)
            clientCnx?.ctx?.writeAndFlush(byteBuf) ?: throw OMSException(ErrorCode.CONNECTION_LOSS.code.toString(), "Connection loss to broker server.")
            this.state.compareAndSet(State.CONNECTING, State.CONNECTED)
        }
    }

    @Throws(OMSException::class)
    override fun sendOneway(message: Message) {

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

    companion object {
        private val SEPARATOR = ";"
    }
}