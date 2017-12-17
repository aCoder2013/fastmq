package com.song.fastmq.client.impl

import java.util.concurrent.ConcurrentHashMap

/**
 * @author song
 */
object RemotingConnectionPoolTest {


    @JvmStatic fun main(args: Array<String>) {
        val map = ConcurrentHashMap<String, String>()
        val test = map.computeIfAbsent("test") {
            it
        }
        println(test)
        println(map["aads"])
    }
}