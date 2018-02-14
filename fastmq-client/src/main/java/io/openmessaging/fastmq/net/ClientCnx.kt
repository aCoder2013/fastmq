package io.openmessaging.fastmq.net

import com.google.common.collect.Lists
import com.song.fastmq.net.AbstractHandler
import com.song.fastmq.net.proto.BrokerApi
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.openmessaging.Message
import io.openmessaging.fastmq.consumer.DefaultPullConsumer
import io.openmessaging.fastmq.domain.BytesMessageImpl
import io.openmessaging.fastmq.domain.MessageId
import io.openmessaging.fastmq.producer.DefaultProducer
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

/**
 * @author song
 */
class ClientCnx : AbstractHandler() {

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

    override fun handleProducerSuccess(commandProducerSuccess: BrokerApi.CommandProducerSuccess, payload: ByteBuf) {
        logger.debug("{} Received producer success response from server: {} - producer-name: {}", ctx?.channel(),
                commandProducerSuccess.requestId, commandProducerSuccess.producerName)
    }

    override fun handleSendError(sendError: BrokerApi.CommandSendError) {
        logger.warn("{} Received send error from server: {}", ctx?.channel(), sendError)
        ctx?.close()
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
        producers[producerId]?.ackReceived(this, sequenceId, ledgerId, entryId) ?:
                logger.warn("Producer[{}] not exist,ignore received message id {}:{}",
                        producerId, ledgerId, entryId)
        logger.debug("{} Got send receipt from producer[{}]: msg---{}, msgId---{}:{}", ctx?.channel(), producerId, sequenceId, ledgerId, entryId)
    }

    fun registerProducer(producerId: Long, producer: DefaultProducer) {
        this.producers.put(producerId, producer)
    }

    fun registerConsumer(consumerId: Long, consumer: DefaultPullConsumer) {
        consumers.put(consumerId, consumer)
    }

    override fun handleSuccess(success: BrokerApi.CommandSuccess) {
        logger.info("success :" + success.toString())
    }

    override fun handleError(error: BrokerApi.CommandError) {
        logger.error("Failed : " + error.toString())
    }

    override fun handleMessage(message: BrokerApi.CommandMessage) {
        logger.info("Received message : " + message.toString())
        val consumerId = message.consumerId
        this.consumers[consumerId]?.let { pullConsumer ->
            val msgs = Lists.newArrayListWithExpectedSize<Message>(message.messagesCount)
            message.messagesList.forEach({
                val msg = BytesMessageImpl()
                it.headersMap.forEach({ k, v ->
                    msg.putHeaders(k, v)
                })
                it.propertiesMap.forEach({ k, v ->
                    msg.putProperties(k, v)
                })
                msg.setBody(it.body.toByteArray())
                msgs.add(msg)
            })
            pullConsumer.receivedMessage(msgs)
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

    companion object {
        private val logger = LoggerFactory.getLogger(ClientCnx::class.java)
    }

}
