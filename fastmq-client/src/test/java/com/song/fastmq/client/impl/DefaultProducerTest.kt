package com.song.fastmq.client.impl

import com.song.fastmq.client.domain.Message
import com.song.fastmq.client.producer.DefaultProducer
import org.junit.Before
import org.junit.Test

/**
 * @author song
 */
class DefaultProducerTest {

    private lateinit var producer: DefaultProducer

//    private val connectionPool = RemotingConnectionPool()

    @Before
    fun setUp() {
//        producer = DefaultProducer(connectionPool)
//        producer.start()
    }

    @Test
    fun send() {
        var i = 0
        while (i++ < 100) {
            val message = Message()
            message.body = "Hello World".toByteArray()
            val sendResult = producer.send(message) ?: throw RuntimeException("Producer shouldn't be null")
//            val messageId = MessageId.fromByteArray(Base64.getDecoder().decode(sendResult.messageId()))
//            println(messageId)
            Thread.sleep(1000)
        }
        Thread.sleep(100000)
    }
}