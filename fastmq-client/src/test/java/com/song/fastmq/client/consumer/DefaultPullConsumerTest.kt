package com.song.fastmq.client.consumer

import com.song.fastmq.storage.common.utils.JsonUtils
import io.openmessaging.BytesMessage
import io.openmessaging.MessagingAccessPointFactory
import io.openmessaging.PullConsumer
import org.junit.Before
import org.junit.Test

/**
 * @author song
 */
class DefaultPullConsumerTest {

    private lateinit var consumer: PullConsumer

    @Before
    fun setUp() {
        val messagingAccessPoint = MessagingAccessPointFactory.getMessagingAccessPoint("openmessaging:fastmq://127.0.0.1:7164/namespace")
        this.consumer = messagingAccessPoint.createPullConsumer("Test-topic-3")
        this.consumer.startup()
    }

    @Test
    fun poll() {
        while (true) {
            val message = this.consumer.poll()
            println(JsonUtils.toJson(message))
            val bytesMessage = message as BytesMessage
            println(String(bytesMessage.body))
        }
    }
}