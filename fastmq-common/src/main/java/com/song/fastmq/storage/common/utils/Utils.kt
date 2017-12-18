package com.song.fastmq.storage.common.utils

import java.net.InetSocketAddress

/**
 * Created by song on 2017/11/5.
 */
object Utils {

    val AVAILABLE_PROCESSORS = Runtime.getRuntime().availableProcessors()

    fun string2SocketAddress(addr: String): InetSocketAddress {
        val s = addr.split(":".toRegex()).dropLastWhile { it.isEmpty() }.toTypedArray()
        return InetSocketAddress(s[0], Integer.parseInt(s[1]))
    }
}
