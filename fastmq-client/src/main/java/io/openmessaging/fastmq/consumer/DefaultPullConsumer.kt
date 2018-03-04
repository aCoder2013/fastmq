package io.openmessaging.fastmq.consumer

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.song.fastmq.net.proto.Commands
import com.song.fastmq.common.domain.FastMQConfigKeys
import com.song.fastmq.common.utils.Utils
import io.netty.buffer.Unpooled
import io.openmessaging.KeyValue
import io.openmessaging.Message
import io.openmessaging.PropertyKeys
import io.openmessaging.PullConsumer
import io.openmessaging.exception.OMSRuntimeException
import io.openmessaging.fastmq.domain.MessageId
import io.openmessaging.fastmq.net.ClientCnx
import io.openmessaging.fastmq.net.RemotingConnectionPool
import io.openmessaging.fastmq.utils.ClientUtils
import org.slf4j.LoggerFactory
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLongFieldUpdater
import java.util.concurrent.atomic.AtomicReference

/**
 * @author song
 */
class DefaultPullConsumer(private val queueName: String, private val properties: KeyValue,
                          private val remotingConnectionPool: RemotingConnectionPool) : PullConsumer {

    private val consumerId: Long = if (properties.containsKey(PropertyKeys.CONSUMER_ID)) {
        this.properties.getLong(PropertyKeys.CONSUMER_ID)
    } else {
        ClientUtils.getNextConsumerId()
    }

    private var consumername: String = if (properties.containsKey(FastMQConfigKeys.CONSUMER_NAME)) {
        this.properties.getString(FastMQConfigKeys.CONSUMER_NAME)
    } else {
        "Consumer@${ClientUtils.buildInstanceName()}@$queueName"
    }

    @Volatile
    private var requestId = 0L

    private val bootstrapServers: ArrayList<String>

    private val state = AtomicReference<State>(State.NONE)

    private var clientCnx: ClientCnx? = null

    private val schedulePullMessagePool: ScheduledExecutorService

    private val messageQueue = LinkedBlockingQueue<Message>(MAX_MESSAGE_CACHE_CAPACITY)

    var readOffset = MessageId.NULL_ID

    init {
        val accessPoints = this.properties.getString(PropertyKeys.ACCESS_POINTS)
        bootstrapServers = ClientUtils.parseBootstrapServers(accessPoints)
        schedulePullMessagePool = Executors
                .newSingleThreadScheduledExecutor(
                        ThreadFactoryBuilder()
                                .setNameFormat("Scheduled-pull-message-pool-%d")
                                .build())
    }

    override fun shutdown() {
        synchronized(this) {
            check(state.get() != State.CLOSED, {
                "Consumer[$consumerId] is already closed!"
            })
            this.schedulePullMessagePool.shutdown()
            this.state.set(State.CLOSED)
        }
    }

    override fun properties(): KeyValue {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun ack(messageId: String?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun ack(messageId: String?, properties: KeyValue?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun startup() {
        if (state.compareAndSet(State.NONE, State.CONNECTING)) {
            clientCnx = remotingConnectionPool.getConnection(Utils.string2SocketAddress(bootstrapServers[0]))
            val subscribe = Commands.newSubscribe(this.queueName, this.consumerId, requestIdGenerator.incrementAndGet(this), this.consumername)
            clientCnx?.let {
                it.ctx?.channel()?.writeAndFlush(Unpooled.wrappedBuffer(subscribe.toByteArray())) ?: throw OMSRuntimeException("-1", "Connect to broker failed!")
                it.registerConsumer(consumerId, this)
            } ?: throw OMSRuntimeException("-1", "Get connection failed!")
            this.state.compareAndSet(State.CONNECTING, State.FETCH_OFFSET)
            val fetchOffset = Commands.newFetchOffset(queueName, consumerId, requestIdGenerator.incrementAndGet(this))
            clientCnx?.let {
                it.ctx?.channel()?.writeAndFlush(Unpooled.wrappedBuffer(fetchOffset.toByteArray())) ?:
                        throw OMSRuntimeException("-1", "Connect to broker failed!")
            }
        } else {
            throw OMSRuntimeException("-1", "Consumer is already starting or has started!")
        }
    }

    @Throws(InterruptedException::class)
    override fun poll(): Message {
        return this.messageQueue.take()
    }

    override fun poll(properties: KeyValue?): Message {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    fun receivedMessage(messages: Iterable<Message>) {
        logger.info("Received message :" + messages)
        messages.forEach({
            if (!messageQueue.offer(it)) {
                //Normal this shouldn't happen because pull worker's flow control
                logger.info("Local message queue is full, discard :{}.", it)
            }
        })
    }

    fun refreshReadOffset(messageId: MessageId) {
        if (this.state.compareAndSet(State.FETCH_OFFSET, State.CONNECTED)) {
            this.readOffset = messageId
            this.schedulePullMessagePool
                    .scheduleAtFixedRate(PullMessageWorker(this), 0L, 200L, TimeUnit.MILLISECONDS)
        } else if (this.state.get() == State.CONNECTED) {
            this.readOffset = messageId
        }
    }

    enum class State {
        NONE,
        CONNECTING,
        CONNECTED,
        FETCH_OFFSET,
        CLOSED
    }

    class PullMessageWorker(private val consumer: DefaultPullConsumer) : Runnable {

        override fun run() {
            try {
                //todo 定时持久化Offset
                val maxMessage = Math.min(MAX_MESSAGE_PULL_NUM, consumer.messageQueue.remainingCapacity())
                if (maxMessage > 0) {
                    val readOffset = consumer.readOffset
                    val command = Commands
                            .newPullMessage(consumer.queueName, consumer.consumerId,
                                    requestIdGenerator.incrementAndGet(consumer),
                                    maxMessage, readOffset.ledgerId, readOffset.entryId)
                    consumer.clientCnx?.ctx?.channel()?.writeAndFlush(Unpooled.wrappedBuffer(command.toByteArray()))
                } else {
                    logger.info("Pull message request is canceled, because there is enough messages in the queue!")
                }
            } catch (e: Exception) {
                logger.error("Pull message failed_" + e.message, e)
            }
        }
    }

    companion object {

        private val logger = LoggerFactory.getLogger(DefaultPullConsumer::class.java)

        private val requestIdGenerator = AtomicLongFieldUpdater.newUpdater(DefaultPullConsumer::class.java, "requestId")

        private val MAX_MESSAGE_CACHE_CAPACITY = 1000

        private val MAX_MESSAGE_PULL_NUM = 32

    }
}