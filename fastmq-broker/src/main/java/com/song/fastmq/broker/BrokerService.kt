package com.song.fastmq.broker

import com.song.fastmq.broker.support.BrokerChannelInitializer
import com.song.fastmq.storage.common.utils.Utils
import com.song.fastmq.storage.storage.MessageStorageFactory
import com.song.fastmq.storage.storage.config.BookKeeperConfig
import com.song.fastmq.storage.storage.impl.MessageStorageFactoryImpl
import io.netty.bootstrap.ServerBootstrap
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel.AdaptiveRecvByteBufAllocator
import io.netty.channel.ChannelOption
import io.netty.channel.EventLoopGroup
import io.netty.channel.epoll.EpollChannelOption
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.epoll.EpollMode
import io.netty.channel.epoll.EpollServerSocketChannel
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.util.concurrent.DefaultThreadFactory
import org.apache.bookkeeper.conf.ClientConfiguration
import org.apache.commons.lang.SystemUtils
import org.slf4j.LoggerFactory
import java.io.Closeable

/**
 * @author song
 */
class BrokerService(private val port: Int = 7164) : Closeable {

    private val messageStorageFactory: MessageStorageFactory

    private val acceptorGroup: EventLoopGroup

    private val workerGroup: EventLoopGroup

    init {
        val clientConfiguration = ClientConfiguration()
        clientConfiguration.zkServers = "127.0.0.1:2181"
        val bkConfig = BookKeeperConfig()
        messageStorageFactory = MessageStorageFactoryImpl(clientConfiguration, bkConfig)
        var acceptorEventLoop: EventLoopGroup
        var workersEventLoop: EventLoopGroup

        val acceptorThreadFactory = DefaultThreadFactory("broker-acceptor")
        val workersThreadFactory = DefaultThreadFactory("broker-io")

        if (SystemUtils.IS_OS_LINUX) {
            try {
                acceptorEventLoop = EpollEventLoopGroup(1, acceptorThreadFactory)
                workersEventLoop = EpollEventLoopGroup(Utils.AVAILABLE_PROCESSORS * 2, workersThreadFactory)
            } catch (e: UnsatisfiedLinkError) {
                acceptorEventLoop = NioEventLoopGroup(1, acceptorThreadFactory)
                workersEventLoop = NioEventLoopGroup(Utils.AVAILABLE_PROCESSORS * 2, workersThreadFactory)
            }
        } else {
            acceptorEventLoop = NioEventLoopGroup(1, acceptorThreadFactory)
            workersEventLoop = NioEventLoopGroup(Utils.AVAILABLE_PROCESSORS * 2, workersThreadFactory)
        }

        this.acceptorGroup = acceptorEventLoop
        this.workerGroup = workersEventLoop
    }

    @Throws(Exception::class)
    fun start() {
        val bootstrap = ServerBootstrap()
        bootstrap.group(acceptorGroup, workerGroup)
                .option(ChannelOption.SO_BACKLOG, 512)
                .option(ChannelOption.SO_REUSEADDR, true)
                .childOption(ChannelOption.TCP_NODELAY, true)
                .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                .childOption(ChannelOption.RCVBUF_ALLOCATOR,
                        AdaptiveRecvByteBufAllocator(1024, 16 * 1024, 1 * 1024 * 1024))
        if (workerGroup is EpollEventLoopGroup) {
            bootstrap.channel(EpollServerSocketChannel::class.java)
            bootstrap.childOption(EpollChannelOption.EPOLL_MODE, EpollMode.LEVEL_TRIGGERED)
        } else {
            bootstrap.channel(NioServerSocketChannel::class.java)
        }

        bootstrap.childHandler(BrokerChannelInitializer(messageStorageFactory))
        bootstrap.bind(port).sync()
        logger.info("Started FastMQ Broker[{}] on port {}.", Utils.getLocalAddress(), port)
    }

    override fun close() {
        acceptorGroup.shutdownGracefully()
        workerGroup.shutdownGracefully()
        logger.info("Broker service shut down.")
    }


    companion object {
        private val logger = LoggerFactory.getLogger(BrokerService::class.java)
    }
}