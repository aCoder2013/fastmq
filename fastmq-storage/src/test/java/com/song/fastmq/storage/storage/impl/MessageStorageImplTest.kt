package com.song.fastmq.storage.storage.impl

import com.song.fastmq.common.message.Message
import com.song.fastmq.common.utils.JsonUtils
import com.song.fastmq.common.utils.OnCompletedObserver
import com.song.fastmq.storage.storage.BatchMessage
import com.song.fastmq.storage.storage.ConsumerInfo
import com.song.fastmq.storage.storage.Offset
import com.song.fastmq.storage.storage.OffsetStorage
import com.song.fastmq.storage.storage.config.BookKeeperConfig
import com.song.fastmq.storage.storage.support.NoMoreMessageException
import org.apache.bookkeeper.client.BookKeeper
import org.apache.bookkeeper.util.OrderedSafeExecutor
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.x.async.AsyncCuratorFramework
import org.apache.logging.log4j.core.config.Configurator
import org.junit.After
import org.junit.Assert
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.runners.MockitoJUnitRunner
import org.slf4j.LoggerFactory
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.fail

/**
 * Created by song on 下午10:02.
 */
@RunWith(MockitoJUnitRunner::class)
class MessageStorageImplTest {

    private lateinit var messageStorage: MessageStorageImpl

    private lateinit var offsetStorage: OffsetStorage

    private lateinit var curatorFramework: CuratorFramework

    private lateinit var bookKeeper: BookKeeper

    @Before
    @Throws(Exception::class)
    fun setUp() {
        Configurator
            .initialize("FastMQ", Thread.currentThread().contextClassLoader, "log4j2.xml")
        val initLatch = CountDownLatch(1)
        curatorFramework = CuratorFrameworkFactory
            .newClient("127.0.0.1:2181", ExponentialBackoffRetry(1000, 3))
        curatorFramework.start()
        val asyncCuratorFramework = AsyncCuratorFramework.wrap(curatorFramework)
        val connectionString = "127.0.0.1:2181"
        val metadataStorage = MetadataStorageImpl(asyncCuratorFramework)
        offsetStorage = ZkOffsetStorageImpl(metadataStorage, asyncCuratorFramework)
        bookKeeper = BookKeeper(connectionString)
        messageStorage = MessageStorageImpl(
            "test", bookKeeper, BookKeeperConfig(), metadataStorage,
            OrderedSafeExecutor.newBuilder().numThreads(Runtime.getRuntime().availableProcessors()).build()
        )
        messageStorage.initialize().blockingSubscribe(object : OnCompletedObserver<Void>() {

            override fun onComplete() {
                initLatch.countDown()
            }

            override fun onError(e: Throwable) {
                e.printStackTrace()
                initLatch.countDown()
            }

        })
        initLatch.await()
    }

    @Test()
    @Throws(Throwable::class)
    fun appendMessage() {
        val total = this.messageStorage.getNumberOfMessages()
        val latch = CountDownLatch(10)
        for (i in 1..10) {
            this.messageStorage.appendMessage(Message(data = "Hello World".toByteArray()))
                .blockingSubscribe({
                    latch.countDown()
                }, {
                    it.printStackTrace()
                    fail(it.message)
                })
        }
        latch.await()
        assertEquals(total + 10, this.messageStorage.numberOfMessages.get())
    }

    @Test
    fun appendMessageAsync() {
        val latch = CountDownLatch(1)
        this.messageStorage.appendMessage(Message(data = "Hello World".toByteArray()))
            .subscribe(object : OnCompletedObserver<Offset>() {

                override fun onNext(t: Offset) {
                    latch.countDown()
                }

                override fun onError(e: Throwable) {
                    latch.countDown()
                    e.printStackTrace()
                    fail(e.message)
                }
            })
        latch.await()
    }

    @Test
    fun queryMessage() {
        val latch = CountDownLatch(1)
        val consumerInfo = ConsumerInfo("consumer-1", "test")
        this.messageStorage.appendMessage(message = Message(data = "Hello World".toByteArray()))
            .blockingSubscribe(object : OnCompletedObserver<Offset>() {

                override fun onNext(t: Offset) {
                    assertNotNull(t)
                    Thread({
                        val offset = offsetStorage.queryOffset(consumerInfo)
                        messageStorage.queryMessage(offset, 100)
                            .subscribe(object : OnCompletedObserver<BatchMessage>() {

                                override fun onNext(t: BatchMessage) {
                                    println(JsonUtils.toJsonQuietly(t))
                                    assertTrue { t.messages.isNotEmpty() }
                                    offsetStorage.commitOffset(consumerInfo, t.nextReadOffset)
                                    offsetStorage.persistOffset(consumerInfo)
                                }

                                override fun onComplete() {
                                    latch.countDown()
                                }

                                override fun onError(e: Throwable) {
                                    e.printStackTrace()
                                    latch.countDown()
                                }
                            })
                    }).start()
                }

                override fun onError(e: Throwable) {
                    e.printStackTrace()
                    latch.countDown()
                }
            })
        latch.await()
    }

    @Test(timeout = 5000L)
    fun queryMessageAppending() {
        val count = AtomicInteger()
        var currentLedgerId = 0L
        val consumerInfo = ConsumerInfo("consumer-1", "test")
        val total = 100
        Thread({
            for (i in 1..total) {
                messageStorage.appendMessage(Message(data = "Hello World : $i".toByteArray()))
                    .blockingSubscribe(object : OnCompletedObserver<Offset>() {

                        override fun onNext(t: Offset) {
                            currentLedgerId = t.ledgerId
                            logger.info("Append message done [{}].", t)
                        }

                        override fun onError(e: Throwable) {
                            e.printStackTrace()
                            Assert.fail(e.message)
                        }
                    })
            }
        }).start()
        var offset = offsetStorage.queryOffset(consumerInfo)
        while (count.get() < total) {
            messageStorage.queryMessage(offset, 100).blockingSubscribe(object : OnCompletedObserver<BatchMessage>() {

                override fun onNext(t: BatchMessage) {
                    offset = t.nextReadOffset
                    t.messages.forEach {
                        if (it.messageId.ledgerId == currentLedgerId) {
                            count.incrementAndGet()
                        }
                        println(it.messageId.ledgerId.toString() + "->" + String(it.data))
                    }
                }

                override fun onError(e: Throwable) {
                    if (e is NoMoreMessageException) {
                        Thread.sleep(50)
                    } else {
                        e.printStackTrace()
                        Assert.fail(e.message)
                    }
                }
            })
        }
    }

    @After
    @Throws(Exception::class)
    fun tearDown() {
        messageStorage.close()
        bookKeeper.close()
        curatorFramework.close()
    }

    companion object {

        private val logger = LoggerFactory.getLogger(MessageStorageImplTest::class.java)
    }
}