package com.song.fastmq.net

import com.song.fastmq.net.proto.BrokerApi
import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import org.slf4j.LoggerFactory
import java.net.SocketAddress

/**
 * @author song
 */
open class AbstractHandler : AbstractMessageDecoder() {

    var ctx: ChannelHandlerContext? = null

    var remoteAddress: SocketAddress? = null

    @Throws(Exception::class)
    override fun channelActive(ctx: ChannelHandlerContext) {
        this.remoteAddress = ctx.channel().remoteAddress()
        this.ctx = ctx
        logger.debug("Channel connect to {} successfully.", this.remoteAddress.toString())
    }

    @Throws(Exception::class)
    override fun channelInactive(ctx: ChannelHandlerContext) {
        super.channelInactive(ctx)
    }

    override fun handleProducerSuccess(commandProducerSuccess: BrokerApi.CommandProducerSuccess, payload: ByteBuf) {
        logger.info("Producer success {}.", commandProducerSuccess)
    }

    companion object {

        private val logger = LoggerFactory.getLogger(AbstractHandler::class.java)
    }

}
