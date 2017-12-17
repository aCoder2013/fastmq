package com.song.fastmq.broker

import com.song.fastmq.broker.support.BrokerChannelInitializer
import com.song.fastmq.storage.common.utils.Utils
import com.song.fastmq.storage.storage.BkLedgerStorage
import com.song.fastmq.storage.storage.config.BookKeeperConfig
import com.song.fastmq.storage.storage.impl.BkLedgerStorageImpl
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
class BrokerService(val port: Int = 7164) : Closeable {

    private val bkLedgerStorage: BkLedgerStorage

    private val acceptorGroup: EventLoopGroup

    private val workerGroup: EventLoopGroup

    init {
        val clientConfiguration = ClientConfiguration()
        clientConfiguration.zkServers = "127.0.0.1:2181"
        val bkConfig = BookKeeperConfig()
        bkLedgerStorage = BkLedgerStorageImpl(clientConfiguration, bkConfig)
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
        bootstrap.childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
        bootstrap.group(acceptorGroup, workerGroup)
        bootstrap.childOption(ChannelOption.TCP_NODELAY, true)
        bootstrap.childOption(ChannelOption.RCVBUF_ALLOCATOR,
                AdaptiveRecvByteBufAllocator(1024, 16 * 1024, 1 * 1024 * 1024))

        if (workerGroup is EpollEventLoopGroup) {
            bootstrap.channel(EpollServerSocketChannel::class.java)
            bootstrap.childOption(EpollChannelOption.EPOLL_MODE, EpollMode.LEVEL_TRIGGERED)
        } else {
            bootstrap.channel(NioServerSocketChannel::class.java)
        }

        bootstrap.childHandler(BrokerChannelInitializer())
        bootstrap.bind(port).sync()
        logger.info("Started FastMQ Broker service on port {}.", port)
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