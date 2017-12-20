package com.song.fastmq.client.impl

import com.song.fastmq.client.BytesMessageImpl
import io.openmessaging.Producer
import io.openmessaging.PropertyKeys
import io.openmessaging.internal.DefaultKeyValue
import org.junit.Before
import org.junit.Test

/**
 * @author song
 */
class DefaultProducerTest {

    var producer: Producer? = null

    @Before
    fun setUp() {
        val properties = DefaultKeyValue()
        properties.put(PropertyKeys.SRC_TOPIC, "Test-topic")
        properties.put(PropertyKeys.ACCESS_POINTS, "127.0.0.1:7164")
        val messagingAccessPoint = MessagingAccessPointImpl(properties)
        producer = messagingAccessPoint.createProducer()
        producer?.startup()
    }


    @Test
    fun send() {
        val message = BytesMessageImpl()
        message.setBody("Hello World".toByteArray())
        producer?.sendOneway(message) ?: throw RuntimeException("Producer shouldn't be null")
        Thread.sleep(100000)
    }
}