package io.openmessaging.fastmq.producer

import com.google.common.base.Preconditions.checkArgument
import com.song.fastmq.net.proto.BrokerApi
import com.song.fastmq.net.proto.Commands
import com.song.fastmq.common.domain.FastMQConfigKeys
import com.song.fastmq.common.utils.Utils
import io.netty.buffer.Unpooled
import io.openmessaging.*
import io.openmessaging.exception.OMSException
import io.openmessaging.exception.OMSMessageFormatException
import io.openmessaging.fastmq.concurrent.DefaultPromise
import io.openmessaging.fastmq.domain.BytesMessageImpl
import io.openmessaging.fastmq.domain.MessageId
import io.openmessaging.fastmq.domain.SendResultImpl
import io.openmessaging.fastmq.exception.ErrorCode
import io.openmessaging.fastmq.exception.FastMqClientException
import io.openmessaging.fastmq.net.ClientCnx
import io.openmessaging.fastmq.net.RemotingConnectionPool
import io.openmessaging.fastmq.utils.ClientUtils
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicLongFieldUpdater
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import java.util.function.Consumer
import kotlin.concurrent.withLock

/**
 * @author song
 */
class DefaultProducer(private val properties: KeyValue, private val cnxPool: RemotingConnectionPool) : Producer {

    private val producerId: Long = if (this.properties.containsKey(PropertyKeys.PRODUCER_ID)) {
        this.properties.getLong(PropertyKeys.PRODUCER_ID)
    } else {
        ClientUtils.getNextProducerId()
    }

    private var name: String = if (this.properties.containsKey(FastMQConfigKeys.PRODUCER_NAME)) {
        this.properties.getString(FastMQConfigKeys.PRODUCER_NAME)
    } else {
        "Producer@${ClientUtils.buildInstanceName()}"
    }

    private var topic: String = this.properties.getString(PropertyKeys.SRC_TOPIC)

    private val bootstrapServers: ArrayList<String>

    private var state = AtomicReference<State>(State.NONE)

    private var cnxClient: ClientCnx? = null

    private val requestIdGenerator = AtomicLong()

    @Volatile
    private var msgIdGenerator = 0L

    private val receivedLock = ReentrantLock()

    private val sendPromises = ConcurrentHashMap<Long, Promise<SendResult>>()

    init {
        checkArgument(this.properties.containsKey(PropertyKeys.ACCESS_POINTS))
        val accessPoints = this.properties.getString(PropertyKeys.ACCESS_POINTS)
        bootstrapServers = ClientUtils.parseBootstrapServers(accessPoints)
    }

    @Throws(Exception::class)
    override fun startup() {
        if (state.compareAndSet(State.NONE, State.CONNECTING)) {
            this.cnxClient = cnxPool.getConnection(Utils.string2SocketAddress(bootstrapServers[0]))
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
        val byteBuf = Unpooled.buffer(toByteArray.size)
        byteBuf.writeBytes(toByteArray)
        cnxClient?.ctx?.writeAndFlush(byteBuf) ?: throw OMSException(ErrorCode.CONNECTION_LOSS.code.toString(), "Connection loss to broker server.")
        this.cnxClient?.registerProducer(producerId, this) ?: throw FastMqClientException("")
    }

    @Throws(OMSException::class)
    override fun sendOneway(message: Message) {
        sendInternal(message, OMS.newKeyValue(), OMS.newKeyValue())
    }

    override fun sendOneway(message: Message, properties: KeyValue) {
        sendInternal(message, OMS.newKeyValue(), properties)
    }

    override fun createBytesMessageToQueue(queue: String, body: ByteArray): BytesMessage {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun properties(): KeyValue {
        return this.properties
    }

    override fun sendAsync(message: Message): Promise<SendResult> {
        return sendInternal(message, OMS.newKeyValue(), OMS.newKeyValue())
    }

    override fun sendAsync(message: Message, properties: KeyValue): Promise<SendResult> {
        return sendInternal(message, OMS.newKeyValue(), properties)
    }

    override fun send(message: Message): SendResult {
        val promise = sendInternal(message, OMS.newKeyValue(), OMS.newKeyValue())
        return promise.get()
    }

    override fun send(message: Message, properties: KeyValue): SendResult {
        val promise = sendInternal(message, OMS.newKeyValue(), properties)
        return promise.get()
    }

    @Throws(Exception::class)
    private fun sendInternal(message: Message, header: KeyValue, properties: KeyValue): Promise<SendResult> {
        val promise = DefaultPromise<SendResult>()
        if (message !is BytesMessage) {
            promise.setFailure(OMSMessageFormatException(ErrorCode.WRONG_MESSAGE_FORMAT.code.toString(), "Only support BytesMessage!"))
            return promise
        }

        header.keySet().forEach(Consumer {
            message.putHeaders(it, header.getString(it))
        })

        message.putHeaders(FastMQConfigKeys.PRODUCER_ID, producerId)
        val sequenceId = msgIdGeneratorUpdater.incrementAndGet(this)
        message.putHeaders(FastMQConfigKeys.SEQUENCE_ID, sequenceId)

        properties.keySet().forEach {
            message.putProperties(it, properties.getString(it))
        }

        val msg = ClientUtils.msgConvert(message)
        cnxClient?.ctx?.let {
            it.writeAndFlush(Unpooled.wrappedBuffer(Commands.newSend(msg).toByteArray()))
            sendPromises.put(sequenceId, promise)
        }
        return promise
    }

    fun ackReceived(clientCnx: ClientCnx, sequenceId: Long, ledgerId: Long, entryId: Long) {
        receivedLock.withLock {
            this.sendPromises[sequenceId]?.let {
                logger.info("ack received")
                val sendResult = SendResultImpl(MessageId(ledgerId, entryId))
                it.set(sendResult)
                this.sendPromises.remove(sequenceId)
            } ?: logger.warn("Ignore invalid send receipt of producer[{}] : msg---{},msgId---{}:{} ", producerId, sequenceId, ledgerId, entryId)
        }
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

        private val logger = LoggerFactory.getLogger(DefaultProducer::class.java)

        private val msgIdGeneratorUpdater = AtomicLongFieldUpdater.newUpdater(DefaultProducer::class.java, "msgIdGenerator")

    }
}