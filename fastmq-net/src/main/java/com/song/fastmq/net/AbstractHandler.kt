package com.song.fastmq.net

import io.netty.channel.ChannelHandlerContext
import org.slf4j.LoggerFactory
import java.net.SocketAddress

/**
 * @author song
 */
open class AbstractHandler : AbstractMessageDecoder() {

    lateinit var ctx: ChannelHandlerContext

    var remoteAddress: SocketAddress? = null

    @Throws(Exception::class)
    override fun channelActive(ctx: ChannelHandlerContext) {
        super.channelActive(ctx)
        this.remoteAddress = ctx.channel().remoteAddress()
        this.ctx = ctx
        logger.info("Channel connect to {} successfully.", this.remoteAddress.toString())
    }

    @Throws(Exception::class)
    override fun channelInactive(ctx: ChannelHandlerContext) {
        super.channelInactive(ctx)
    }

    companion object {

        private val logger = LoggerFactory.getLogger(AbstractHandler::class.java)
    }

}
