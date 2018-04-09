package com.song.fastmq.client.consumer

import com.google.common.base.Preconditions.checkArgument
import com.google.common.collect.Lists
import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.song.fastmq.client.domain.Message
import com.song.fastmq.client.domain.MessageId
import com.song.fastmq.client.exception.FastMqClientException
import com.song.fastmq.client.net.ClientCnx
import com.song.fastmq.client.net.RemotingConnectionPool
import com.song.fastmq.client.utils.ClientUtils
import com.song.fastmq.common.utils.Utils
import com.song.fastmq.net.proto.Commands
import io.netty.buffer.Unpooled
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
class DefaultPullConsumer(
    private val topic: String,
    private val remotingConnectionPool: RemotingConnectionPool
) : PullConsumer {

    private val consumerId: Long = ClientUtils.getNextConsumerId()

    /**
     * Should be globally unique
     */
    private var consumerName: String = ""

    @Volatile
    private var requestId = 0L

    private val bootstrapServers = ArrayList<String>()

    private val state = AtomicReference<State>(State.NONE)

    private lateinit var clientCnx: ClientCnx

    private val schedulePullMessagePool: ScheduledExecutorService = Executors
        .newSingleThreadScheduledExecutor(
            ThreadFactoryBuilder()
                .setNameFormat("Scheduled-pull-message-pool-%d")
                .build()
        )

    private val messageQueue = LinkedBlockingQueue<Message>(MAX_MESSAGE_CACHE_CAPACITY)

    var readOffset = MessageId.NULL_ID

    override fun start() {
        checkArgument(consumerName.isBlank(), "Consumer name can't be null or empty, it should be globally unique.")
        if (state.compareAndSet(State.NONE, State.CONNECTING)) {
            clientCnx = remotingConnectionPool.getConnection(Utils.string2SocketAddress(bootstrapServers[0]))
            val subscribe = Commands.newSubscribe(
                this.topic,
                this.consumerId,
                requestIdGenerator.incrementAndGet(this),
                this.consumerName
            )
            clientCnx.registerConsumer(consumerId, this)
            clientCnx.sendCommandAsync(Unpooled.wrappedBuffer(subscribe.toByteArray()), ClientUtils.getNextRequestId())
                .thenAccept {
                    synchronized(this@DefaultPullConsumer) {
                        if (state.get() == State.CLOSING || state.get() == State.CLOSED) {
                            clientCnx.removeConsumer(consumerId)
                            clientCnx.channel().close()
                            return@thenAccept
                        }
                        logger.info(
                            "[{}] [{}] Created consumer on cnx {},start to fetch offset",
                            topic,
                            consumerName,
                            clientCnx.channel()
                        )
                        clientCnx.ctx.writeAndFlush(
                            Unpooled
                                .wrappedBuffer(
                                    Commands
                                        .newFetchOffset(topic, consumerId, ClientUtils.getNextRequestId()).toByteArray()
                                )
                        )
                        state.set(State.CONNECTED)
                    }
                }
                .exceptionally {
                    synchronized(this@DefaultPullConsumer) {
                        logger.error("[$topic] [$consumerName] Failed to create consumer:{}", it)
                        clientCnx.removeConsumer(consumerId)
                        if (state.get() == State.CLOSING || state.get() == State.CLOSED) {
                            clientCnx.channel().close()
                            return@exceptionally null
                        }
                        state.set(State.FAILED)
                    }
                    return@exceptionally null
                }
        } else {
            throw FastMqClientException("Consumer is already starting or has started!")
        }
    }

    override fun poll(): List<Message> {
        val messages = Lists.newArrayList<Message>()
        this.messageQueue.drainTo(messages, MAX_MESSAGE_PULL_NUM)
        return messages
    }

    override fun poll(pullCallback: PullCallback) {
        pullCallback.onSuccess(poll())
    }

    override fun updateConsumeOffset(messageId: MessageId) {
        logger.info("[{}] [{}] update consumer offset to :{}.", topic, consumerName, messageId)
        this.readOffset = messageId
    }

    fun receivedMessage(messages: Iterable<Message>) {
        synchronized(this@DefaultPullConsumer) {
            logger.info("Received message :{}.", messages)
            messages.forEach({
                if (!messageQueue.offer(it)) {
                    //Normal this won't happen because pull worker's flow control
                    logger.warn("Local message queue is full, discard :{}.", it)
                }
            })
        }
    }

    fun refreshReadOffset(messageId: MessageId) {
        if (this.state.compareAndSet(State.FETCH_OFFSET, State.CONNECTED)) {
            this.readOffset = messageId
            this.schedulePullMessagePool
                .scheduleAtFixedRate(PullMessageWorker(this), 0L, 200L, TimeUnit.MILLISECONDS)
            logger.info("[{}] [{}] Start to pull message from offset :{}.", topic, clientCnx.channel(), readOffset)
        } else if (this.state.get() == State.CONNECTED) {
            synchronized(this@DefaultPullConsumer) {
                this.readOffset = messageId
                logger.info("[{}] [{}] update consumer offset to :{}.", topic, consumerName, messageId)
            }
        }
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

    enum class State {
        NONE,
        CONNECTING,
        CONNECTED,
        FETCH_OFFSET,
        CLOSING,
        CLOSED,
        FAILED
    }

    class PullMessageWorker(private val consumer: DefaultPullConsumer) : Runnable {

        override fun run() {
            try {
                //todo 定时持久化Offset
                val maxMessage = Math.min(MAX_MESSAGE_PULL_NUM, consumer.messageQueue.remainingCapacity())
                if (maxMessage > 0) {
                    val readOffset = consumer.readOffset
                    val command = Commands
                        .newPullMessage(
                            consumer.topic, consumer.consumerId,
                            requestIdGenerator.incrementAndGet(consumer),
                            maxMessage, readOffset.ledgerId, readOffset.entryId
                        )
                    consumer.clientCnx.ctx.writeAndFlush(Unpooled.wrappedBuffer(command.toByteArray()))
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

        private const val MAX_MESSAGE_CACHE_CAPACITY = 1000

        private const val MAX_MESSAGE_PULL_NUM = 32
    }
}