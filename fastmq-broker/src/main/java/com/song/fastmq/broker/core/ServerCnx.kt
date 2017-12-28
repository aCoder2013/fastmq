package com.song.fastmq.broker.core

import com.google.common.base.Preconditions.checkArgument
import com.song.fastmq.broker.core.persistent.PersistentTopic
import com.song.fastmq.net.AbstractHandler
import com.song.fastmq.net.proto.BrokerApi
import com.song.fastmq.net.proto.Commands
import com.song.fastmq.storage.common.domain.FastMQConfigKeys
import com.song.fastmq.storage.storage.BkLedgerStorage
import com.song.fastmq.storage.storage.LogManager
import com.song.fastmq.storage.storage.LogReader
import com.song.fastmq.storage.storage.Version
import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks
import com.song.fastmq.storage.storage.support.LedgerStorageException
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * @author song
 */
class ServerCnx(private val bkLedgerStorage: BkLedgerStorage) : AbstractHandler() {

    private var state = State.NONE

    private val producers = ConcurrentHashMap<Long, CompletableFuture<Producer>>()

    private val consumers = ConcurrentHashMap<Long, CompletableFuture<Consumer>>()

    init {
        state = State.START
    }

    enum class State {
        NONE,
        START,
        CONNECTED
    }

    override fun channelActive(ctx: ChannelHandlerContext) {
        super.channelActive(ctx)
        state = State.CONNECTED
        logger.info("New Connection from $remoteAddress")
    }

    override fun channelInactive(ctx: ChannelHandlerContext) {
        super.channelInactive(ctx)
        producers.values.forEach({
            try {
                if (it.isDone && !it.isCompletedExceptionally) {
                    val producer = it.getNow(null)
                    producer.close()
                }
            } catch (e: Exception) {
                logger.error("Close producer failed,maybe already closed", e)
            }
        })
        producers.clear()
    }

    override fun channelWritabilityChanged(ctx: ChannelHandlerContext?) {
        super.channelWritabilityChanged(ctx)
        logger.info("Channel writability has changed to ${ctx?.channel()?.isWritable}")
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext?, cause: Throwable?) {
        logger.error("Got exception $remoteAddress,${cause?.message}", cause)
        ctx?.close()
    }

    override fun handleProducer(commandProducer: BrokerApi.CommandProducer, payload: ByteBuf) {
        checkArgument(state == State.CONNECTED)
        var producerName: String = if (commandProducer.producerName.isNullOrBlank()) {
            UUID.randomUUID().toString().replace("-", "")
        } else {
            commandProducer.producerName
        }
        val topicName = commandProducer.topic
        val producerId = commandProducer.producerId
        val requestId = commandProducer.requestId

        val existingProducerFuture = producers[producerId]
        if (existingProducerFuture != null) {
            if (existingProducerFuture.isDone && !existingProducerFuture.isCompletedExceptionally) {
                val producer = existingProducerFuture.getNow(null)
                logger.info("[{}] Producer with id  is already created :", remoteAddress, producer)
            } else {
                logger.warn("[{}] Producer is already present on the broker.", remoteAddress)
                return
            }
        }
        logger.info("[{}][{}] Try to create producer with id [{}]", remoteAddress, topicName, producerId)
        val future = CompletableFuture<Producer>()
        bkLedgerStorage.asyncOpen(topicName, object : AsyncCallbacks.CommonCallback<LogManager, LedgerStorageException> {
            override fun onCompleted(data: LogManager, version: Version) {
                val producer = Producer(PersistentTopic(topicName, data), this@ServerCnx, producerName, producerId)
                future.complete(producer)
            }

            override fun onThrowable(throwable: LedgerStorageException) {
                future.completeExceptionally(throwable)
                throwable.printStackTrace()
            }
        })
        this.producers.putIfAbsent(producerId, future)
        val producerSuccess = BrokerApi.CommandProducerSuccess.newBuilder()
                .setProducerName(UUID.randomUUID().toString().replace("-", ""))
                .setRequestId(0L).build()
        val command = BrokerApi.Command.newBuilder().setProducerSuccess(producerSuccess).setType(BrokerApi.Command.Type.PRODUCER_SUCCESS).build()
        ctx?.channel()?.writeAndFlush(Unpooled.wrappedBuffer(command.toByteArray()))
    }

    override fun handleSend(commandSend: BrokerApi.CommandSend) {
        if (commandSend.headersMap[FastMQConfigKeys.PRODUCER_ID].isNullOrBlank()
                || commandSend.headersMap[FastMQConfigKeys.SEQUENCE_ID].isNullOrBlank()) {
            logger.warn("Ignore invalid message ,{}", commandSend.toString())
            return
        }
        val producerId = commandSend.headersMap[FastMQConfigKeys.PRODUCER_ID]!!.toLong()
        val sequenceId = commandSend.headersMap[FastMQConfigKeys.SEQUENCE_ID]!!.toLong()
        val producerFuture = this.producers[producerId]
        if (producerFuture == null || producerFuture.isCompletedExceptionally) {
            logger.warn("[{}] Producer had already been closed: {},cause :{}", remoteAddress, producerId)
            return
        }
        try {
            val producer = producerFuture.get(3, TimeUnit.SECONDS)
            producer.publishMessage(producerId, sequenceId, Unpooled.wrappedBuffer(commandSend.toByteArray()))
        } catch (e: TimeoutException) {
            logger.info("[{}] Create producer timeout after 3S,{}", remoteAddress, producerId)
        }
    }

    override fun handleSubscribe(subscribe: BrokerApi.CommandSubscribe) {
        val topic = subscribe.topic
        val consumerId = subscribe.consumerId
        val consumerName = subscribe.consumerName
        val requestId = subscribe.requestId

        val logManager = this.bkLedgerStorage.open(topic)
        val future = CompletableFuture<Consumer>()
        val existingFuture = this.consumers.putIfAbsent(consumerId, future)
        if (existingFuture != null) {
            if (existingFuture.isDone && !existingFuture.isCompletedExceptionally) {
                val consumer = existingFuture.getNow(null)
                logger.info("[{}] Consumer with the same id is already created: {}", remoteAddress, consumer)
                ctx?.writeAndFlush(Commands.newSuccess(requestId))
                return
            } else {
                ctx?.writeAndFlush(Unpooled.wrappedBuffer(Commands.newError(requestId, BrokerApi.ServerError.UnknownError, "Consumer is already present on the connection").toByteArray()))
                return
            }
        }
        logManager.asyncOpenCursor(consumerName, object : AsyncCallbacks.OpenCursorCallback {
            override fun onComplete(logReader: LogReader) {
                future.complete(Consumer(logReader))
                ctx?.channel()?.writeAndFlush(Unpooled.wrappedBuffer(Commands.newSuccess(requestId).toByteArray()))
            }

            override fun onThrowable(throwable: Throwable) {
                logger.error(throwable.message, throwable)
                future.completeExceptionally(throwable)
                ctx?.channel()?.writeAndFlush(Unpooled.wrappedBuffer(Commands.newError(requestId, BrokerApi.ServerError.UnknownError, "Consumer is already present on the connection").toByteArray()))
                consumers.remove(consumerId)
                return
            }
        })
    }

    override fun handlePullMessage(pullMessage: BrokerApi.CommandPullMessage) {
        val consumerId = pullMessage.consumerId
        this.consumers[consumerId]?.let {
            if (it.isDone && !it.isCompletedExceptionally) {
                val consumer = it.getNow(null)
                val messages = consumer.readMessage(pullMessage.maxMessage)
                ctx?.channel()?.writeAndFlush(Unpooled.wrappedBuffer(Commands.newMessage(consumerId, messages).toByteArray()))
            } else {
                ctx?.writeAndFlush(Unpooled.wrappedBuffer(Commands.newError(pullMessage.requestId, BrokerApi.ServerError.UnknownError, "Consumer is not ready!").toByteArray()))
            }
        } ?: logger.error("Consumer not exist :{} ", consumerId)
    }

    companion object {

        private val logger = LoggerFactory.getLogger(ServerCnx::class.java)

    }
}