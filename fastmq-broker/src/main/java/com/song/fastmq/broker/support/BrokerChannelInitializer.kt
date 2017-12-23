package com.song.fastmq.broker.support

import com.song.fastmq.broker.core.ServerCnxHandler
import com.song.fastmq.storage.storage.BkLedgerStorage
import io.netty.channel.ChannelInitializer
import io.netty.channel.socket.SocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder

/**
 * @author song
 */
class BrokerChannelInitializer(val bkLedgerStorage: BkLedgerStorage) : ChannelInitializer<SocketChannel>() {


    override fun initChannel(ch: SocketChannel) {
        ch.pipeline().addLast("frameDecoder", LengthFieldBasedFrameDecoder(5 * 1024 * 1024, 0, 4, 0, 4))
        ch.pipeline().addLast("handler", ServerCnxHandler(bkLedgerStorage))
    }

}
