package com.song.fastmq.broker.support

import com.song.fastmq.broker.core.ServerCnxClient
import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder

/**
 * @author song
 */
class BrokerChannelInitializer : ChannelInitializer<SocketChannel>() {


    override fun initChannel(ch: SocketChannel) {
        ch.pipeline().addLast("frameDecoder", LengthFieldBasedFrameDecoder(5 * 1024 * 1024, 0, 4, 0, 4))
        ch.pipeline().addLast("handler", ServerCnxClient())
    }

}
