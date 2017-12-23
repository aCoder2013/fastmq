package com.song.fastmq.broker

import org.junit.Before
import org.junit.Test

/**
 * @author song
 */
class BrokerServiceTest {

    private var brokerService: BrokerService = BrokerService()

    @Before
    fun setUp() {
        brokerService.start()
    }

    @Test
    fun start() {
        Thread.sleep(10000000)
    }

    @Test
    fun close() {
        brokerService.close()
    }

}