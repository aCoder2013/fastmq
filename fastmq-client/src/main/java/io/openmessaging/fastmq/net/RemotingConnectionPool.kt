package io.openmessaging.fastmq.net

import com.song.fastmq.common.logging.LoggerFactory
import com.song.fastmq.common.utils.Utils
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
import io.netty.handler.codec.LengthFieldPrepender
import io.netty.handler.logging.LogLevel
import io.netty.handler.logging.LoggingHandler
import io.netty.util.concurrent.DefaultThreadFactory
import org.apache.commons.lang3.SystemUtils
import java.io.Closeable
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap

/**
 * @author song
 */
class RemotingConnectionPool : Closeable {

    private val pool: ConcurrentHashMap<InetSocketAddress, ClientCnx> = ConcurrentHashMap()

    private val bootstrap: Bootstrap
    private val eventLoopGroup: EventLoopGroup

    private val MaxMessageSize = 5 * 1024 * 1024

    private val nettyClientConfig = NettyClientConfig()

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
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, nettyClientConfig.connectTimeoutMillis)
        bootstrap.handler(object : ChannelInitializer<SocketChannel>() {

            @Throws(Exception::class)
            public override fun initChannel(ch: SocketChannel) {
                ch.pipeline().addLast(LoggingHandler(LogLevel.INFO))
                ch.pipeline().addLast(LengthFieldPrepender(4))
                ch.pipeline().addLast(LengthFieldBasedFrameDecoder(MaxMessageSize, 0, 4, 0, 4))
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

    private fun getEventLoopGroup(): EventLoopGroup {
        val numThreads = Utils.AVAILABLE_PROCESSORS
        val threadFactory = DefaultThreadFactory("fastmq-client-io")

        return try {
            EpollEventLoopGroup(numThreads, threadFactory)
        } catch (e: ExceptionInInitializerError) {
            logger.debug("Unable to load EpollEventLoop", e)
            NioEventLoopGroup(numThreads, threadFactory)
        } catch (e: NoClassDefFoundError) {
            logger.debug("Unable to load EpollEventLoop", e)
            NioEventLoopGroup(numThreads, threadFactory)
        } catch (e: UnsatisfiedLinkError) {
            logger.debug("Unable to load EpollEventLoop", e)
            NioEventLoopGroup(numThreads, threadFactory)
        }
    }

    companion object {
        private val logger = LoggerFactory.getLogger(RemotingConnectionPool::class.java)
    }

}