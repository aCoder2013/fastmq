package com.song.fastmq.client.net

import com.google.common.collect.Lists
import com.song.fastmq.client.consumer.DefaultPullConsumer
import com.song.fastmq.client.domain.Message
import com.song.fastmq.client.domain.MessageId
import com.song.fastmq.client.exception.FastMqClientException
import com.song.fastmq.client.producer.DefaultProducer
import com.song.fastmq.net.AbstractHandler
import com.song.fastmq.net.proto.BrokerApi
import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import io.netty.channel.ChannelHandlerContext
import org.slf4j.LoggerFactory
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap

/**
 * @author song
 */
class ClientCnx : AbstractHandler() {

    private val pendingRequests = ConcurrentHashMap<Long, CompletableFuture<Pair<String, Long>>>()

    private val producers = ConcurrentHashMap<Long, DefaultProducer>()

    private val consumers = ConcurrentHashMap<Long, DefaultPullConsumer>()

    override fun channelActive(ctx: ChannelHandlerContext) {
        super.channelActive(ctx)
        logger.info("Connected to broker {}.", ctx.channel())
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        logger.warn("[{}] Exception caught:{}", ctx.channel(), cause.message, cause)
        ctx.close()
    }

    fun sendCommandAsync(cmd: ByteBuf, requestId: Long): CompletableFuture<Pair<String, Long>> {
        val future = CompletableFuture<Pair<String, Long>>()
        pendingRequests[requestId] = future
        ctx.writeAndFlush(cmd).addListener {
            if (!it.isSuccess) {
                logger.warn("{} Failed to send command to broker :{}.", ctx.channel(), it.cause().message)
                pendingRequests.remove(requestId)
                future.completeExceptionally(it.cause())
            }
        }
        return future
    }

    fun registerProducer(producerId: Long, producer: DefaultProducer) {
        this.producers[producerId] = producer
    }

    fun removeProducer(producerId: Long) {
        this.producers.remove(producerId)
    }

    fun registerConsumer(consumerId: Long, consumer: DefaultPullConsumer) {
        consumers[consumerId] = consumer
    }

    fun removeConsumer(consumerId: Long) {
        consumers.remove(consumerId)
    }

    override fun handleProducerSuccess(commandProducerSuccess: BrokerApi.CommandProducerSuccess, payload: ByteBuf) {
        logger.debug("{} Received producer success response from server: {} - producer-name: {}", ctx?.channel(),
                commandProducerSuccess.requestId, commandProducerSuccess.producerName)
    }

    override fun handleSendError(sendError: BrokerApi.CommandSendError) {
        logger.warn("{} Received send error from server: {}", ctx.channel(), sendError)
        ctx.close()
    }

    override fun handleSendReceipt(sendReceipt: BrokerApi.CommandSendReceipt) {
        val producerId = sendReceipt.producerId
        val sequenceId = sendReceipt.sequenceId
        var ledgerId = -1L
        var entryId = -1L
        if (sendReceipt.hasMessageId()) {
            ledgerId = sendReceipt.messageId.ledgerId
            entryId = sendReceipt.messageId.entryId
        }
        producers[producerId]?.ackReceived(sequenceId, ledgerId, entryId)
                ?: logger.warn("Producer[{}] not exist,ignore received message id {}:{}",
                        producerId, ledgerId, entryId)
        logger.debug("{} Got send receipt from producer[{}]: msg---{}, msgId---{}:{}", ctx.channel(), producerId, sequenceId, ledgerId, entryId)
    }

    override fun handleSuccess(success: BrokerApi.CommandSuccess) {
        val completableFuture = this.pendingRequests[success.requestId]
        completableFuture?.run {
            complete(null)
        } ?: run {
            logger.warn("Receive unknown success command,no match requestId :.", success.requestId)
        }
    }

    override fun handleError(error: BrokerApi.CommandError) {
        logger.error("[${ctx.channel()}] Received error from server: ${error.message}")
        val requestId = error.requestId
        val future = pendingRequests[requestId]
        future?.completeExceptionally(FastMqClientException(error.message))
                ?: logger.warn("{} Received unknown request id from server: {}", ctx.channel(), error.requestId)
    }

    override fun handleMessage(message: BrokerApi.CommandMessage) {
        logger.info("Received message : " + message.toString())
        val consumerId = message.consumerId
        this.consumers[consumerId]?.let { pullConsumer ->
            val messages = Lists.newArrayListWithExpectedSize<Message>(message.messagesCount)
            message.messagesList.forEach({
                val msg = Message()
                msg.properties.putAll(it.headersMap)
                msg.body = it.body.toByteArray()
                messages.add(msg)
            })
            pullConsumer.receivedMessage(messages)
            pullConsumer.refreshReadOffset(MessageId(message.nextReadOffset.ledgerId, message.nextReadOffset.entryId))
        } ?: logger.warn("Consumer[{}] not exist,just ignore!", consumerId)
    }

    override fun handleFetchOffsetResponse(fetchOffsetResponse: BrokerApi.CommandFetchOffsetResponse) {
        val consumerId = fetchOffsetResponse.consumerId
        val pullConsumer = this.consumers[consumerId]
        val messageId = fetchOffsetResponse.messageId
        if (pullConsumer != null) {
            logger.info("PullConsumer[{}] Update read offset from {} to {}", pullConsumer.readOffset, messageId)
            pullConsumer.refreshReadOffset(MessageId(messageId.ledgerId, messageId.entryId))
        } else {
            logger.warn("Consumer[{}] with offset ledgerId = {} entryId = {}  not exist.", consumerId, messageId.ledgerId, messageId.entryId)
        }
    }

    fun channel(): Channel {
        return ctx.channel()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ClientCnx::class.java)
    }

}
