package com.song.fastmq.client.impl

import com.song.fastmq.storage.common.utils.Utils
import io.netty.bootstrap.Bootstrap
import io.netty.buffer.PooledByteBufAllocator
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.EventLoopGroup
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.epoll.EpollSocketChannel
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioSocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.util.concurrent.DefaultThreadFactory
import org.apache.commons.lang3.SystemUtils
import org.slf4j.LoggerFactory
import java.io.Closeable
import java.net.InetSocketAddress
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ConcurrentHashMap

/**
 * @author song
 */
class RemotingConnectionPool : Closeable {

    private val pool: ConcurrentHashMap<InetSocketAddress, CompletableFuture<ClientCnxClient>> = ConcurrentHashMap()

    private val bootstrap: Bootstrap
    private val eventLoopGroup: EventLoopGroup

    private val MaxMessageSize = 5 * 1024 * 1024

    init {
        this.eventLoopGroup = getEventLoopGroup()
        bootstrap = Bootstrap()
        bootstrap.group(eventLoopGroup)
        if (SystemUtils.IS_OS_LINUX && eventLoopGroup is EpollEventLoopGroup) {
            bootstrap.channel(EpollSocketChannel::class.java)
        } else {
            bootstrap.channel(NioSocketChannel::class.java)
        }

        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000)
        bootstrap.option(ChannelOption.TCP_NODELAY, true)
        bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)

        bootstrap.handler(object : ChannelInitializer<SocketChannel>() {
            @Throws(Exception::class)
            public override fun initChannel(ch: SocketChannel) {
                ch.pipeline().addLast("frameDecoder", LengthFieldBasedFrameDecoder(MaxMessageSize, 0, 4, 0, 4))
                ch.pipeline().addLast("handler", ClientCnxClient())
            }
        })
    }

    fun getConnection(address: InetSocketAddress): CompletableFuture<ClientCnxClient> {
        return this.pool.computeIfAbsent(address) {
            createConnection(it)
        }
    }

    private fun createConnection(address: InetSocketAddress): CompletableFuture<ClientCnxClient> {
        val connectionFuture = CompletableFuture<ClientCnxClient>()
        val channelFuture = bootstrap.connect(address).sync()
        connectionFuture.complete(channelFuture.channel().pipeline().get("handler") as ClientCnxClient)
//        bootstrap.connect(address).addListener {
//            ChannelFutureListener { future ->
//                logger.info("Create connection")
//                if (!future.isSuccess) {
//                    connectionFuture.completeExceptionally(FastMqClientException(future.cause()))
//                    return@ChannelFutureListener
//                }
//                logger.info("[{}] connected to server .", future.channel())
//                val cnx = future.channel().pipeline().get("handler") as ClientCnxClient
//                connectionFuture.complete(cnx)
//            }
//        }
        return connectionFuture
    }

    override fun close() {
        this.eventLoopGroup.shutdownGracefully()
    }

    private fun getEventLoopGroup(): EventLoopGroup {
        val numThreads = Utils.AVAILABLE_PROCESSORS
        val threadFactory = DefaultThreadFactory("fastmq-client-io")

        try {
            return EpollEventLoopGroup(numThreads, threadFactory)
        } catch (e: ExceptionInInitializerError) {
            logger.debug("Unable to load EpollEventLoop", e)
            return NioEventLoopGroup(numThreads, threadFactory)
        } catch (e: NoClassDefFoundError) {
            logger.debug("Unable to load EpollEventLoop", e)
            return NioEventLoopGroup(numThreads, threadFactory)
        } catch (e: UnsatisfiedLinkError) {
            logger.debug("Unable to load EpollEventLoop", e)
            return NioEventLoopGroup(numThreads, threadFactory)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RemotingConnectionPool::class.java)
    }

}