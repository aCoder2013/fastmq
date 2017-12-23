package io.openmessaging.fastmq.impl

import io.openmessaging.MessagingAccessPointFactory
import io.openmessaging.Producer
import io.openmessaging.PropertyKeys
import io.openmessaging.fastmq.domain.BytesMessageImpl
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
        val messagingAccessPoint = MessagingAccessPointFactory.getMessagingAccessPoint("openmessaging:fastmq://127.0.0.1:7164/namespace")
        val properties = DefaultKeyValue()
        properties.put(PropertyKeys.SRC_TOPIC, "Test-topic")
        properties.put(PropertyKeys.PRODUCER_ID, System.currentTimeMillis())
        producer = messagingAccessPoint.createProducer(properties)
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