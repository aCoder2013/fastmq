package com.song.fastmq.net.proto

import org.junit.Test

/**
 * @author song
 */
class CommandsTest {

    @Test
    fun newSendReceipt() {
        val sendReceipt = Commands.newSendReceipt(1L, 2L, 3L, 4L)
        println(sendReceipt)
        val mergeFrom = BrokerApi.Command.newBuilder().mergeFrom(sendReceipt.toByteArray()).build()
        println(mergeFrom)
    }
}