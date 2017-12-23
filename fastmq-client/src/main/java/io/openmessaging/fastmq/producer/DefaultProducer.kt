package io.openmessaging.fastmq.producer

import com.google.common.base.Preconditions.checkArgument
import com.song.fastmq.net.proto.BrokerApi
import com.song.fastmq.storage.common.utils.Utils
import io.netty.buffer.Unpooled
import io.netty.channel.Channel
import io.openmessaging.*
import io.openmessaging.exception.OMSException
import io.openmessaging.fastmq.FastMQConfigKeys
import io.openmessaging.fastmq.domain.BytesMessageImpl
import io.openmessaging.fastmq.exception.ErrorCode
import io.openmessaging.fastmq.exception.FastMqClientException
import io.openmessaging.fastmq.net.RemotingConnectionPool
import io.openmessaging.fastmq.utils.ClientUtils
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import java.util.function.Consumer

/**
 * @author song
 */
class DefaultProducer(private val properties: KeyValue, private val cnxPool: RemotingConnectionPool) : Producer {

    private val producerId: Long

    private var name: String

    private var topic: String

    private val bootstrapServers = ArrayList<String>()

    private var state = AtomicReference<State>(State.NONE)

    private var cnxClient: Channel? = null

    private val requestIdGenerator = AtomicLong()

    init {
        if (this.properties.containsKey(FastMQConfigKeys.PRODUCER_NAME)) {
            name = this.properties.getString(FastMQConfigKeys.PRODUCER_NAME)
        } else {
            name = ClientUtils.buildInstanceName()
        }
        if (this.properties.containsKey(PropertyKeys.PRODUCER_ID)) {
            this.producerId = this.properties.getLong(PropertyKeys.PRODUCER_ID)
        } else {
            this.producerId = ClientUtils.getNextProducerId()
        }
        topic = this.properties.getString(PropertyKeys.SRC_TOPIC)
        checkArgument(this.properties.containsKey(PropertyKeys.ACCESS_POINTS))
        val accessPoints = this.properties.getString(PropertyKeys.ACCESS_POINTS)
        accessPoints.split(SEPARATOR).forEach(Consumer {
            bootstrapServers.add(it)
        })
        checkArgument(bootstrapServers.isNotEmpty())
    }

    @Throws(Exception::class)
    override fun startup() {
        if (state.compareAndSet(State.NONE, State.CONNECTING)) {
            val clientCnx = cnxPool.getConnection(Utils.string2SocketAddress(bootstrapServers[0]))
            val channelFuture = clientCnx.channelFuture
            channelFuture.awaitUninterruptibly()
            if (channelFuture.isCancelled) {
                throw FastMqClientException("Start producer failed,because client canceled the connect request.")
            } else if (!channelFuture.isSuccess) {
                throw channelFuture.cause()
            } else if (clientCnx.isOK) {
                this.cnxClient = clientCnx.channel
            }
            registerProducer()
            this.state.compareAndSet(State.CONNECTING, State.CONNECTED)
        } else {
            throw FastMqClientException("Producer state is not right ,maybe started before ,$state")
        }
    }

    private fun registerProducer() {
        val producer = BrokerApi.CommandProducer
                .newBuilder()
                .setProducerId(this.producerId)
                .setProducerName(name)
                .setTopic(topic)
                .setRequestId(requestIdGenerator.incrementAndGet())
                .build()
        val command = BrokerApi.Command.newBuilder().setProducer(producer).setType(BrokerApi.Command.Type.PRODUCER).build()
        val toByteArray = command.toByteArray()
//        val byteBuf = Unpooled.buffer(4 + toByteArray.size)
        val byteBuf = Unpooled.buffer(toByteArray.size)
//        val size = toByteArray.size
//        byteBuf.writeInt(size)
        byteBuf.writeBytes(toByteArray)
        cnxClient?.writeAndFlush(byteBuf) ?: throw OMSException(ErrorCode.CONNECTION_LOSS.code.toString(), "Connection loss to broker server.")
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