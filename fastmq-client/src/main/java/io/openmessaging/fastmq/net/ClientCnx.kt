package io.openmessaging.fastmq.net

import com.song.fastmq.net.AbstractHandler
import com.song.fastmq.net.proto.BrokerApi
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import org.slf4j.LoggerFactory

/**
 * @author song
 */
class ClientCnx : AbstractHandler() {

    override fun channelActive(ctx: ChannelHandlerContext) {
        super.channelActive(ctx)
        logger.info("Connected to broker {}.", ctx.channel())
    }

    override fun channelInactive(ctx: ChannelHandlerContext) {
        super.channelInactive(ctx)
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
        logger.error("error :" + sendError)
    }

    override fun handleSendReceipt(sendReceipt: BrokerApi.CommandSendReceipt) {
        logger.info("发送成功啦!" + sendReceipt)
    }

    companion object {
        private val logger = LoggerFactory.getLogger(ClientCnx::class.java)
    }

}
