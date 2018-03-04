package io.openmessaging.fastmq.impl

import io.openmessaging.MessagingAccessPointFactory
import io.openmessaging.Producer
import io.openmessaging.PropertyKeys
import io.openmessaging.fastmq.domain.BytesMessageImpl
import io.openmessaging.fastmq.domain.MessageId
import io.openmessaging.internal.DefaultKeyValue
import org.junit.After
import org.junit.Before
import org.junit.Test
import java.util.*

/**
 * @author song
 */
class DefaultProducerTest {

    private lateinit var producer: Producer

    @Before
    fun setUp() {
        val messagingAccessPoint = MessagingAccessPointFactory.getMessagingAccessPoint("openmessaging:fastmq://127.0.0.1:7164/namespace")
        val properties = DefaultKeyValue()
        properties.put(PropertyKeys.SRC_TOPIC, "Test-topic-3")
        properties.put(PropertyKeys.PRODUCER_ID, System.currentTimeMillis())
        producer = messagingAccessPoint.createProducer(properties)
        producer.startup()
    }

    @Test
    fun send() {
        var i = 0
        while (i++ < 100) {
            val message = BytesMessageImpl()
            message.setBody("Hello World".toByteArray())
            val sendResult = producer.send(message) ?: throw RuntimeException("Producer shouldn't be null")
            val messageId = MessageId.fromByteArray(Base64.getDecoder().decode(sendResult.messageId()))
            println(messageId)
            Thread.sleep(1000)
        }
    }

    @After
    fun tearDown() {
        this.producer.shutdown()
    }
}