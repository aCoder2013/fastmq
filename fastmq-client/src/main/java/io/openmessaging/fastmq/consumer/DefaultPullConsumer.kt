package io.openmessaging.fastmq.consumer

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.song.fastmq.net.proto.Commands
import com.song.fastmq.storage.common.domain.FastMQConfigKeys
import com.song.fastmq.storage.common.utils.Utils
import io.netty.buffer.Unpooled
import io.openmessaging.KeyValue
import io.openmessaging.Message
import io.openmessaging.PropertyKeys
import io.openmessaging.PullConsumer
import io.openmessaging.exception.OMSRuntimeException
import io.openmessaging.fastmq.net.ClientCnx
import io.openmessaging.fastmq.net.RemotingConnectionPool
import io.openmessaging.fastmq.utils.ClientUtils
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLongFieldUpdater
import java.util.concurrent.atomic.AtomicReference

/**
 * @author song
 */
class DefaultPullConsumer(private val queueName: String, private val properties: KeyValue, private val remotingConnectionPool: RemotingConnectionPool) : PullConsumer {

    private val consumerId: Long = if (properties.containsKey(PropertyKeys.CONSUMER_ID)) {
        this.properties.getLong(PropertyKeys.CONSUMER_ID)
    } else {
        ClientUtils.getNextConsumerId()
    }

    private var consumername: String = if (properties.containsKey(FastMQConfigKeys.CONSUMER_NAME)) {
        this.properties.getString(FastMQConfigKeys.CONSUMER_NAME)
    } else {
        "Consumer@${ClientUtils.buildInstanceName()}"
    }

    @Volatile
    private var requestId = 0L

    private val bootstrapServers: ArrayList<String>

    private val state = AtomicReference<State>(State.NONE)

    private var clientCnx: ClientCnx? = null

    private val schedulePullMessagePool: ScheduledExecutorService

    private val messageQueue = ConcurrentLinkedQueue<Message>()

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
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
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
            this.schedulePullMessagePool.scheduleAtFixedRate(Worker(this), 0L, 10L, TimeUnit.SECONDS)
            this.state.compareAndSet(State.CONNECTING, State.CONNECTED)
        } else {
            throw OMSRuntimeException("-1", "Consumer is already starting or has started!")
        }
    }

    override fun poll(): Message {
        while (true) {
            if (this.messageQueue.isEmpty()) {
                Thread.sleep(100)
            } else {
                return this.messageQueue.poll()
            }
        }
    }

    override fun poll(properties: KeyValue?): Message {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    fun receivedMessage(messages: Iterable<Message>) {
        logger.info("Received message :" + messages)
        this.messageQueue.addAll(messages)
    }

    enum class State {
        NONE,
        CONNECTING,
        CONNECTED
    }

    class Worker(private val consumer: DefaultPullConsumer) : Runnable {

        override fun run() {
            try {
                val command = Commands.newPullMessage(consumer.queueName, consumer.consumerId, requestIdGenerator.incrementAndGet(consumer), 20)
                consumer.clientCnx?.ctx?.channel()?.writeAndFlush(Unpooled.wrappedBuffer(command.toByteArray()))
            } catch (e: Exception) {
                logger.error("Pull message failed_" + e.message, e)
            }
        }

    }

    companion object {

        private val logger = LoggerFactory.getLogger(DefaultPullConsumer::class.java)

        private val requestIdGenerator = AtomicLongFieldUpdater.newUpdater(DefaultPullConsumer::class.java, "requestId")

    }
}