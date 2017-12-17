package com.song.fastmq.client

import java.io.Closeable

/**
 * @author song
 */
interface Producer : Closeable {

    fun start()

    fun setServer(servers: String)

    fun send(message: ByteArray): MessageId

    @Throws(Exception::class)
    override fun close()
}