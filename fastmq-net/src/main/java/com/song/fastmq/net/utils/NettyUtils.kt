package com.song.fastmq.net.utils

import io.netty.channel.EventLoopGroup
import io.netty.channel.epoll.Epoll
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import java.util.concurrent.ThreadFactory

/**
 * @author song
 */
object NettyUtils {

    /**
     * @return an EventLoopGroup based on the current platform
     */
    fun newEventLoopGroup(nThreads: Int, threadFactory: ThreadFactory): EventLoopGroup {
        return if (Epoll.isAvailable()) {
            EpollEventLoopGroup(nThreads, threadFactory)
        } else {
            // Fallback to NIO
            NioEventLoopGroup(nThreads, threadFactory)
        }
    }
}
