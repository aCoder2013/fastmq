package com.song.fastmq.client.impl

import org.junit.Before
import org.junit.Test

/**
 * @author song
 */
class DefaultProducerTest {

    private val producer = DefaultProducer("HelloTopic")

    @Before
    fun setUp() {
        producer.setServer("127.0.0.1:7164")
        producer.start()
    }

    @Test
    fun start() {
        val messageId = producer.send("Hello".toByteArray())
        println(messageId)
        Thread.sleep(10000000)
    }

    @Test
    fun send() {
    }

}