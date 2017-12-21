package com.song.fastmq.broker.core

import com.google.common.base.Preconditions.checkArgument
import com.song.fastmq.broker.core.persistent.PersistentTopic
import com.song.fastmq.net.AbstractHandler
import com.song.fastmq.net.proto.BrokerApi
import com.song.fastmq.storage.storage.BkLedgerStorage
import com.song.fastmq.storage.storage.LogManager
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

/**
 * @author song
 */
class ServerCnxClient(val bkLedgerStorage: BkLedgerStorage) : AbstractHandler() {

    private var state = State.NONE

    private val producers = ConcurrentHashMap<Long, CompletableFuture<Producer>>()

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
        var producerName: String
        if (commandProducer.producerName.isNullOrBlank()) {
            producerName = UUID.randomUUID().toString().replace("-", "")
        } else {
            producerName = commandProducer.producerName
        }
        val topicName = commandProducer.topic
        val producerId = commandProducer.producerId
        val requestId = commandProducer.requestId

        val producerFuture = CompletableFuture<Producer>()
        val existingProducerFuture = producers.putIfAbsent(producerId, producerFuture)
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
                val producer = Producer(PersistentTopic(topicName, data), this@ServerCnxClient, producerName, producerId)
                future.complete(producer)
            }

            override fun onThrowable(throwable: LedgerStorageException) {
                future.completeExceptionally(throwable)
            }
        })
        this.producers.putIfAbsent(producerId, future)
        val producerSuccess = BrokerApi.CommandProducerSuccess.newBuilder()
                .setProducerName(UUID.randomUUID().toString().replace("-", ""))
                .setRequestId(0L).build()
        val command = BrokerApi.Command.newBuilder().setProducerSuccess(producerSuccess).setType(BrokerApi.Command.Type.PRODUCER_SUCCESS).build()
        val body = command.toByteArray()
        val byteBuf = Unpooled.buffer(4 + body.size)
        byteBuf.writeInt(body.size)
        byteBuf.writeBytes(body)
        ctx?.channel()?.writeAndFlush(byteBuf)
    }

    companion object {

        private val logger = LoggerFactory.getLogger(ServerCnxClient::class.java)

    }
}