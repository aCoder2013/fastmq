package com.song.fastmq.broker.core

import com.google.common.base.Preconditions.checkArgument
import com.google.common.base.Throwables
import com.song.fastmq.broker.core.persistent.PersistentTopic
import com.song.fastmq.net.AbstractHandler
import com.song.fastmq.net.proto.BrokerApi
import com.song.fastmq.net.proto.Commands
import com.song.fastmq.storage.common.domain.FastMQConfigKeys
import com.song.fastmq.storage.common.utils.OnCompletedObserver
import com.song.fastmq.storage.storage.ConsumerInfo
import com.song.fastmq.storage.storage.MessageStorage
import com.song.fastmq.storage.storage.Offset
import com.song.fastmq.storage.storage.impl.MessageStorageFactoryImpl
import com.song.fastmq.storage.storage.support.OffsetStorageException
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.netty.channel.ChannelHandlerContext
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeoutException

/**
 * @author song
 */
class ServerCnx(private val messageStorageFactory: MessageStorageFactoryImpl) : AbstractHandler() {

    private var state = State.NONE

    private val producers = ConcurrentHashMap<Long, Producer>()

    private val consumers = ConcurrentHashMap<Long, Consumer>()

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
                it.close()
            } catch (e: Exception) {
                logger.error("Close producer failed,maybe already closed", e)
            }
        })
        producers.clear()
    }

    override fun channelWritabilityChanged(ctx: ChannelHandlerContext) {
        super.channelWritabilityChanged(ctx)
        logger.info("Channel writability has changed to ${ctx.channel().isWritable}")
    }

    override fun exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
        logger.error("Got exception $remoteAddress,${cause.message}", cause)
        ctx.close()
    }

    override fun handleProducer(commandProducer: BrokerApi.CommandProducer, payload: ByteBuf) {
        checkArgument(state == State.CONNECTED)
        val producerName: String = if (commandProducer.producerName.isNullOrBlank()) {
            UUID.randomUUID().toString().replace("-", "")
        } else {
            commandProducer.producerName
        }
        val topic = commandProducer.topic
        val producerId = commandProducer.producerId
        val requestId = commandProducer.requestId

        var existingProducer = producers[producerId]
        if (existingProducer == null) {
            logger.info("[{}][{}] Try to create producer with id [{}]", remoteAddress, topic, producerId)
            this.messageStorageFactory.open(topic).subscribe(object : OnCompletedObserver<MessageStorage>() {
                override fun onError(e: Throwable) {
                    logger.error("Open producer failed_" + e.message, e)
                }

                override fun onNext(t: MessageStorage) {
                    producers.putIfAbsent(producerId, Producer(PersistentTopic(topic, t), this@ServerCnx, producerName, producerId))
                }

                override fun onComplete() {
                }
            })
        }

        val producerSuccess = BrokerApi.CommandProducerSuccess
                .newBuilder()
                .setProducerName(UUID.randomUUID().toString().replace("-", ""))
                .setRequestId(0L).build()
        val command = BrokerApi.Command
                .newBuilder()
                .setProducerSuccess(producerSuccess)
                .setType(BrokerApi.Command.Type.PRODUCER_SUCCESS).build()
        ctx.writeAndFlush(Unpooled.wrappedBuffer(command.toByteArray()))
    }

    override fun handleSend(commandSend: BrokerApi.CommandSend) {
        if (commandSend.headersMap[FastMQConfigKeys.PRODUCER_ID].isNullOrBlank()
                || commandSend.headersMap[FastMQConfigKeys.SEQUENCE_ID].isNullOrBlank()) {
            logger.warn("Ignore invalid message ,{}", commandSend.toString())
            return
        }
        val producerId = commandSend.headersMap[FastMQConfigKeys.PRODUCER_ID]!!.toLong()
        val sequenceId = commandSend.headersMap[FastMQConfigKeys.SEQUENCE_ID]!!.toLong()
        val producer = this.producers[producerId]
        if (producer == null) {
            logger.warn("[{}] Producer doesn't exist [{}].", remoteAddress, producerId)
            return
        }
        try {
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

        val consumer = this.consumers[consumerId]
        if (consumer == null) {
            this.messageStorageFactory.open(topic)
                    .subscribe(object : OnCompletedObserver<MessageStorage>() {
                        override fun onError(e: Throwable) {
                            logger.error("[$topic][$consumerId] Open message storage failed_" + e.message, e)
                            ctx.writeAndFlush(Unpooled.wrappedBuffer(Commands
                                    .newError(requestId,
                                            BrokerApi.ServerError.UnknownError,
                                            Throwables.getStackTraceAsString(e)).toByteArray()))
                        }

                        override fun onNext(t: MessageStorage) {
                            val previous = consumers.putIfAbsent(consumerId, Consumer(this@ServerCnx, t))
                            if (previous != null) {
                                ctx.writeAndFlush(Unpooled.wrappedBuffer(Commands
                                        .newError(requestId,
                                                BrokerApi.ServerError.UnknownError,
                                                "Consumer is already present on the connection").toByteArray()))
                            } else {
                                ctx.writeAndFlush(Unpooled.wrappedBuffer(Commands.newSuccess(requestId).toByteArray()))
                            }
                        }
                    })
        }
    }

    override fun handlePullMessage(pullMessage: BrokerApi.CommandPullMessage) {
        val consumerId = pullMessage.consumerId
        val messageId = pullMessage.messageId
        this.consumers[consumerId]?.readMessage(consumerId, Offset(messageId.ledgerId, messageId.entryId), pullMessage.maxMessage) ?: run {
            logger.error("Consumer not exist :{} ", consumerId)
            ctx.writeAndFlush(Unpooled.wrappedBuffer(Commands
                    .newError(pullMessage.requestId,
                            BrokerApi.ServerError.UnknownError, "Consumer is not ready!").toByteArray()))
        }
    }

    override fun handleFetchOffset(fetchOffset: BrokerApi.CommandFetchOffset) {
        try {
            val offset = messageStorageFactory.offsetStorage
                    .queryOffset(ConsumerInfo(fetchOffset.consumerName, fetchOffset.topic))
            ctx.writeAndFlush(Unpooled.wrappedBuffer(Commands
                    .newFetchOffsetResponse(fetchOffset.topic, fetchOffset.consumerId,
                            offset.ledgerId, offset.entryId).toByteArray()))
        } catch (e: OffsetStorageException) {
            logger.error(e.message, e)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ServerCnx::class.java)
    }
}