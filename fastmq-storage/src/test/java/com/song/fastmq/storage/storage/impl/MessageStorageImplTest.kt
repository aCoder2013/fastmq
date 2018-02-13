package com.song.fastmq.storage.storage.impl

import com.song.fastmq.storage.common.message.Message
import com.song.fastmq.storage.common.utils.OnCompletedObserver
import com.song.fastmq.storage.storage.ConsumerInfo
import com.song.fastmq.storage.storage.GetMessageResult
import com.song.fastmq.storage.storage.OffsetStorage
import com.song.fastmq.storage.storage.config.BookKeeperConfig
import org.apache.bookkeeper.client.BookKeeper
import org.apache.bookkeeper.util.OrderedSafeExecutor
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.x.async.AsyncCuratorFramework
import org.apache.logging.log4j.core.config.Configurator
import org.junit.After
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.runners.MockitoJUnitRunner
import org.slf4j.LoggerFactory
import java.util.concurrent.CountDownLatch
import kotlin.test.assertEquals
import kotlin.test.fail

/**
 * Created by song on 下午10:02.
 */
@RunWith(MockitoJUnitRunner::class)
class MessageStorageImplTest {

    private lateinit var messageStorage: MessageStorageImpl

    private lateinit var offsetStorage: OffsetStorage

    @Before
    @Throws(Exception::class)
    fun setUp() {
        Configurator
                .initialize("FastMQ", Thread.currentThread().contextClassLoader, "log4j2.xml")
        val initLatch = CountDownLatch(1)
        val curatorFramework = CuratorFrameworkFactory
                .newClient("127.0.0.1:2181", ExponentialBackoffRetry(1000, 3))
        curatorFramework.start()
        val asyncCuratorFramework = AsyncCuratorFramework.wrap(curatorFramework)
        val connectionString = "127.0.0.1:2181"
        val metadataStorage = MetadataStorageImpl(asyncCuratorFramework)
        offsetStorage = ZkOffsetStorageImpl(metadataStorage, asyncCuratorFramework)
        messageStorage = MessageStorageImpl("test", BookKeeper(connectionString), BookKeeperConfig(), metadataStorage,
                OrderedSafeExecutor.newBuilder().numThreads(Runtime.getRuntime().availableProcessors()).build())
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
        val total = this.messageStorage.numberOfEntries.get()
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
        assertEquals(total + 10, this.messageStorage.numberOfEntries.get())
    }

    @Test
    fun queryMessage() {
        val consumerInfo = ConsumerInfo("consumer-1", "test")
        val offset = offsetStorage.queryOffset(consumerInfo)
        this.messageStorage.queryMessage(offset, 100)
                .blockingSubscribe(object : OnCompletedObserver<GetMessageResult>() {

                    override fun onNext(t: GetMessageResult) {
                        t.messages.forEach {
                            println(it.messageId.toString() + ":" + String(it.data))
                        }
                        offsetStorage.commitOffset(consumerInfo, t.nextReadOffset)
                        offsetStorage.persistOffset(consumerInfo)
                    }

                    override fun onComplete() {
                    }

                    override fun onError(e: Throwable) {
                        e.printStackTrace()
                        fail(e.message)
                    }
                })
        TODO("当前Ledger读取完成则将游标移动到下一个")
    }

    @After
    @Throws(Exception::class)
    fun tearDown() {
        messageStorage.close()
    }

    companion object {

        private val logger = LoggerFactory.getLogger(MessageStorageImplTest::class.java)
    }
}