package com.song.fastmq.client.producer

import com.google.common.base.Preconditions.checkArgument
import com.song.fastmq.client.domain.Message
import com.song.fastmq.client.domain.MessageId
import com.song.fastmq.client.exception.FastMqClientException
import com.song.fastmq.client.net.ClientCnx
import com.song.fastmq.client.net.RemotingConnectionPool
import com.song.fastmq.client.utils.ClientUtils
import com.song.fastmq.common.domain.MessageConstants
import com.song.fastmq.common.utils.Utils
import com.song.fastmq.net.proto.Commands
import io.netty.buffer.Unpooled
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicLongFieldUpdater
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * @author song
 */
class DefaultProducer : Producer {

    private var topic: String

    /**
     * Should be globally unique
     */
    private var producerName: String

    private val cnxPool: RemotingConnectionPool

    constructor(cnxPool: RemotingConnectionPool, topic: String = "", producerName: String = "") {
        this.cnxPool = cnxPool
        this.topic = topic
        this.producerName = producerName
        this.producerId = ClientUtils.getNextProducerId()
        this.state = AtomicReference(State.NONE)
        this.pendingRequest = ConcurrentHashMap()
        this.receivedLock = ReentrantLock()
    }

    private var producerId: Long

    private lateinit var bootstrapServers: ArrayList<String>

    private val state: AtomicReference<State>

    private lateinit var cnxClient: ClientCnx

    private val pendingRequest: ConcurrentHashMap<Long, CompletableFuture<MessageId>>

    @Volatile
    private var msgIdGenerator = 0L

    private val receivedLock: ReentrantLock

    override fun start() {
        if (state.compareAndSet(State.NONE, State.CONNECTING)) {
            checkArgument(bootstrapServers.size > 0)
            checkArgument(topic.isNotBlank())
            this.cnxClient = cnxPool.getConnection(Utils.string2SocketAddress(bootstrapServers[0]))
            connect()
            this.state.compareAndSet(State.CONNECTING, State.CONNECTED)
        } else {
            throw FastMqClientException("Producer state is not right ,maybe started before ,$state")
        }
    }

    private fun connect() {
        this.cnxClient.registerProducer(producerId, this)
        cnxClient
                .sendCommandAsync(Unpooled.wrappedBuffer(Commands
                        .newProducer(topic, producerId, producerName, ClientUtils.getNextRequestId()).toByteArray()),
                        ClientUtils.getNextRequestId())
                .thenAccept({
                    val producerName = it.first
                    synchronized(this@DefaultProducer) {
                        if (state.get() == State.CLOSING || state.get() == State.CLOSED) {
                            cnxClient.removeProducer(producerId)
                            cnxClient.channel().close()
                            return@thenAccept
                        }
                        logger.info("[{}] [{}] Created producer on cnx {}", topic, producerName, cnxClient.channel())
                        if (this.producerName.isBlank()) {
                            this.producerName = producerName
                        }
                        this.state.compareAndSet(State.CONNECTING, State.CONNECTED)
                    }
                }).exceptionally {
                    synchronized(this@DefaultProducer) {
                        logger.error("[$topic] [$producerName] Failed to create producer:{}", it)
                        cnxClient.removeProducer(producerId)
                        if (state.get() == State.CLOSING || state.get() == State.CLOSED) {
                            cnxClient.channel().close()
                            return@exceptionally null
                        }
                        state.set(State.FAILED)
                    }
                    return@exceptionally null
                }
    }

    override fun send(message: Message): MessageId? {
        try {
            return sendAsync(message).get()
        } catch (e: InterruptedException) {
            Thread.currentThread().interrupt()
        } catch (e: ExecutionException) {
            throw FastMqClientException(e.cause)
        }
        return null
    }

    override fun sendAsync(message: Message): CompletableFuture<MessageId> {
        val future = CompletableFuture<MessageId>()
        val sequenceId = msgIdGeneratorUpdater.getAndIncrement(this)
        this.pendingRequest[sequenceId] = future
        message.properties[MessageConstants.SEQUENCE_ID] = sequenceId.toString()
        message.properties[MessageConstants.PRODUCER_ID] = producerId.toString()
        cnxClient.ctx.writeAndFlush(Unpooled.wrappedBuffer(ClientUtils.msgConvert(message).toByteArray()))
                .addListener { it ->
                    if (!it.isSuccess) {
                        this.pendingRequest.remove(sequenceId)
                        future.completeExceptionally(it.cause())
                        return@addListener
                    }
                }
        return future
    }

    override fun getProducerName() = this.producerName

    fun ackReceived(sequenceId: Long, ledgerId: Long, entryId: Long) {
        receivedLock.withLock {
            val completableFuture = this.pendingRequest[sequenceId]
            if (completableFuture != null) {
                if (completableFuture.isDone) {
                    logger.warn("[{}][{}] Request[{}] is already done.", topic, producerName, sequenceId)
                } else {
                    completableFuture.complete(MessageId(ledgerId, entryId))
                }
                this.pendingRequest.remove(sequenceId)
            } else {
                logger.warn("Ignore invalid send receipt of producer[{}] : msg---{},msgId---{}:{} ", producerId, sequenceId, ledgerId, entryId)
            }
        }
    }

    override fun shutdown() {
        state.set(State.NONE)
    }

    enum class State {
        NONE,
        CONNECTING,
        CONNECTED,
        CLOSING,
        CLOSED,
        FAILED
    }

    companion object {

        private val logger = LoggerFactory.getLogger(DefaultProducer::class.java)

        private val msgIdGeneratorUpdater = AtomicLongFieldUpdater.newUpdater(DefaultProducer::class.java, "msgIdGenerator")

    }
}