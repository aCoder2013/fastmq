package com.song.fastmq.storage.storage.impl

import com.song.fastmq.storage.storage.MetadataStorage
import com.song.fastmq.storage.storage.concurrent.CommonPool
import com.song.fastmq.storage.storage.metadata.LogSegment
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.x.async.details.AsyncCuratorFrameworkImpl
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooKeeper
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import java.util.*
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger

/**
 * @author song
 */
class MetadataStorageImplTest {

    private lateinit var zookeeper: ZooKeeper

    private lateinit var metadataStorage: MetadataStorage

    @Before
    @Throws(Exception::class)
    fun setUp() {
        val latch = CountDownLatch(1)
        zookeeper = ZooKeeper("127.0.0.1:2181", 10000) { event ->
            if (event.state == Watcher.Event.KeeperState.SyncConnected) {
                println("Zookeeper connected.")
            } else {
                throw RuntimeException("Error connecting to zookeeper")
            }
            latch.countDown()
        }
        latch.await()
        val curatorFramework = CuratorFrameworkFactory.newClient("127.0.0.1:2181", ExponentialBackoffRetry(1000, 3))
        curatorFramework.start()
        val asyncCuratorFramework = AsyncCuratorFrameworkImpl(curatorFramework)
        metadataStorage = MetadataStorageImpl(asyncCuratorFramework)
    }

    @Test
    @Throws(Exception::class)
    fun getLogInfo() {
        val name = "HelloWorldTest1"
        metadataStorage.getLogInfo(name).blockingSubscribe {
            Assert.assertEquals(name, it.name)
        }
    }

    @Test
    fun getLogInfoAsync() {
        val latch = CountDownLatch(1)
        val name = "HelloWorldTest1"
        metadataStorage.getLogInfo(name).subscribe {
            latch.countDown()
            Assert.assertEquals(name, it.name)
        }
        latch.await()
    }

    @Test
    @Throws(Exception::class)
    fun asyncUpdateLedgerStream() {
        val latch = CountDownLatch(1)
        val counter = AtomicInteger()
        val name = "HelloWorldTest1"
        metadataStorage.getLogInfo(name).subscribe {
            CommonPool.executeBlocking(Runnable {
                it.segments = Collections.singletonList(LogSegment())
                metadataStorage.updateLogInfo(name, it).blockingSubscribe {
                    counter.incrementAndGet()
                    latch.countDown()
                }
            })
        }
        latch.await()
        Assert.assertEquals(1, counter.get().toLong())
    }

    @Test
    @Throws(Exception::class)
    fun asyncRemoveLedger() {
        val latch = CountDownLatch(1)
        val counter = AtomicInteger()
        metadataStorage.removeLogInfo("HelloWorldTest1").subscribe {
            counter.incrementAndGet()
            latch.countDown()
        }
        latch.await()
        Assert.assertEquals(1, counter.get().toLong())
    }

    @After
    @Throws(Exception::class)
    fun tearDown() {
        zookeeper.close()
    }
}