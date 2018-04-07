package com.song.fastmq.client.net

import com.song.fastmq.client.core.conf.ClientConfiguration
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.EventLoopGroup
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.epoll.EpollSocketChannel
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.LengthFieldPrepender
import io.netty.handler.logging.LogLevel
import io.netty.handler.logging.LoggingHandler
import org.apache.commons.lang3.SystemUtils
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap

/**
 * @author song
 */
class RemotingConnectionPool(private val configuration: ClientConfiguration, private val eventLoopGroup: EventLoopGroup) : Closeable {

    private val pool: ConcurrentHashMap<InetSocketAddress, ClientCnx> = ConcurrentHashMap()

    private val bootstrap: Bootstrap = Bootstrap()

    private val nettyClientConfig = NettyClientConfig()

    init {
        bootstrap.group(eventLoopGroup)
        if (SystemUtils.IS_OS_LINUX && eventLoopGroup is EpollEventLoopGroup) {
            bootstrap.channel(EpollSocketChannel::class.java)
        } else {
            bootstrap.channel(NioSocketChannel::class.java)
        }

        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
        bootstrap.option(ChannelOption.TCP_NODELAY, configuration.useTcpNoDelay)
        bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.connectTimeoutMillis)
        bootstrap.handler(object : ChannelInitializer<SocketChannel>() {

            @Throws(Exception::class)
            public override fun initChannel(ch: SocketChannel) {
                ch.pipeline().addLast(LoggingHandler(LogLevel.INFO))
                ch.pipeline().addLast(LengthFieldPrepender(4))
                ch.pipeline().addLast(LengthFieldBasedFrameDecoder(MAX_MESSAGE_SIZE, 0, 4, 0, 4))
                ch.pipeline().addLast("handler", ClientCnx())
            }
        })
    }

    fun getConnection(address: InetSocketAddress): ClientCnx {
        return this.pool.computeIfAbsent(address) {
            createConnection(it)
        }
    }

    private fun createConnection(address: InetSocketAddress): ClientCnx {
        val channelFuture = bootstrap.connect(address).syncUninterruptibly()
        return channelFuture.channel().pipeline().get("handler") as ClientCnx
    }

    override fun close() {
        this.eventLoopGroup.shutdownGracefully()
    }


    companion object {

        const val MAX_MESSAGE_SIZE = 5 * 1024 * 1024

        private val logger = LoggerFactory.getLogger(RemotingConnectionPool::class.java)

    }

}