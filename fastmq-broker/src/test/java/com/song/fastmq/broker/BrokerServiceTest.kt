package com.song.fastmq.broker

import org.apache.logging.log4j.core.config.Configurator
import org.junit.Before
import org.junit.Test

/**
 * @author song
 */
class BrokerServiceTest {

    private lateinit var brokerService: BrokerService

    @Before
    fun setUp() {
        brokerService = BrokerService()
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

    companion object {
        init {
            Configurator.initialize("FastMQ", Thread.currentThread().contextClassLoader, "log4j2.xml")
        }
    }

}