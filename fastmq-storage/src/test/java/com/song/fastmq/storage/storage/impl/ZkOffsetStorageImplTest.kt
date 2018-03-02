package com.song.fastmq.storage.storage.impl

import com.song.fastmq.storage.storage.ConsumerInfo
import com.song.fastmq.storage.storage.MetadataStorage
import com.song.fastmq.storage.storage.Offset
import com.song.fastmq.storage.storage.OffsetStorage
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.x.async.AsyncCuratorFramework
import org.apache.logging.log4j.core.config.Configurator
import org.junit.After
import org.junit.Assert.assertEquals
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Mock
import org.mockito.runners.MockitoJUnitRunner

/**
 * @author song
 */
@RunWith(MockitoJUnitRunner::class)
class ZkOffsetStorageImplTest {

    @Mock
    private lateinit var metadataStorage: MetadataStorage

    private lateinit var offsetStorage: OffsetStorage

    private lateinit var curatorFramework: CuratorFramework

    @Before
    @Throws(Exception::class)
    fun setUp() {
        Configurator
                .initialize("FastMQ", Thread.currentThread().contextClassLoader, "log4j2.xml")
        curatorFramework = CuratorFrameworkFactory
                .newClient("127.0.0.1:2181", ExponentialBackoffRetry(1000, 3))
        curatorFramework.start()
        val asyncCuratorFramework = AsyncCuratorFramework.wrap(curatorFramework)
        offsetStorage = ZkOffsetStorageImpl(metadataStorage, asyncCuratorFramework)
    }

    @Test
    @Throws(Exception::class)
    fun queryOffset() {
        val consumerInfo = ConsumerInfo("consumer-1", "test")
        /*
            Put into memory cache
         */
        this.offsetStorage.commitOffset(consumerInfo, Offset(10, 1024))
        val offset = this.offsetStorage.queryOffset(consumerInfo)
        assertEquals(10, offset.ledgerId)
        assertEquals(1024, offset.entryId)
    }

    @After
    @Throws(Exception::class)
    fun tearDown() {
        val consumerInfo = ConsumerInfo("consumer-1", "test")
        this.offsetStorage.removeOffset(consumerInfo)
        this.offsetStorage.close()
        this.curatorFramework.close()
    }

}