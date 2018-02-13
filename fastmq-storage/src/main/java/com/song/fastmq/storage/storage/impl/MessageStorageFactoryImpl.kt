package com.song.fastmq.storage.storage.impl

import com.song.fastmq.storage.common.utils.OnCompletedObserver
import com.song.fastmq.storage.storage.MessageStorage
import com.song.fastmq.storage.storage.MessageStorageFactory
import com.song.fastmq.storage.storage.MetadataStorage
import com.song.fastmq.storage.storage.OffsetStorage
import com.song.fastmq.storage.storage.config.BookKeeperConfig
import com.song.fastmq.storage.storage.support.LedgerStorageException
import io.reactivex.Observable
import io.reactivex.ObservableEmitter
import org.apache.bookkeeper.client.BookKeeper
import org.apache.bookkeeper.conf.ClientConfiguration
import org.apache.bookkeeper.util.OrderedSafeExecutor
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.x.async.AsyncCuratorFramework
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooKeeper
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * Default implementation of [MessageStorageFactory]
 * Created by song on 2017/11/4.
 */
class MessageStorageFactoryImpl @Throws(Exception::class)
constructor(clientConfiguration: ClientConfiguration, private val bookKeeperConfig: BookKeeperConfig) : MessageStorageFactory {

    @Volatile
    private var closed: Boolean = false

    private val zooKeeper: ZooKeeper

    private val bookKeeper: BookKeeper

    private val asyncCuratorFramework: AsyncCuratorFramework

    private val metadataStorage: MetadataStorage

    private val offsetStorage: OffsetStorage

    private val messageStorageCache = ConcurrentHashMap<String, MessageStorage>()

    private val curatorFramework: CuratorFramework

    private val messageOrderedThreadPool = OrderedSafeExecutor
            .newBuilder()
            .name("Message-ordered-thread-pool=%d")
            .numThreads(20)
            .build()

    init {
        val servers = clientConfiguration.zkServers
        val countDownLatch = CountDownLatch(1)

        zooKeeper = ZooKeeper(servers, clientConfiguration.zkTimeout) { event ->
            if (event.state == Watcher.Event.KeeperState.SyncConnected) {
                logger.info("Connected to zookeeper ,connectString = {}", servers)
                countDownLatch.countDown()
            } else {
                logger.error("Failed to connect zookeeper,connectString = {}", servers)
            }
        }
        if (!countDownLatch.await(clientConfiguration.zkTimeout.toLong(), TimeUnit.MILLISECONDS) || zooKeeper.state != ZooKeeper.States.CONNECTED) {
            throw LedgerStorageException(
                    "Error connecting to zookeeper server ,connectString = $servers.")
        }

        this.bookKeeper = BookKeeper(clientConfiguration, zooKeeper)
        val retryPolicy = ExponentialBackoffRetry(1000, 3)
        curatorFramework = CuratorFrameworkFactory.newClient(servers, retryPolicy)
        curatorFramework.start()
        asyncCuratorFramework = AsyncCuratorFramework.wrap(curatorFramework)
        metadataStorage = MetadataStorageImpl(asyncCuratorFramework)
        offsetStorage = ZkOffsetStorageImpl(metadataStorage, asyncCuratorFramework)
    }

    override fun open(topic: String): Observable<MessageStorage> {
        return Observable.create<MessageStorage> { observable: ObservableEmitter<MessageStorage> ->
            if (this.messageStorageCache.contains(topic)) {
                observable.onNext(this.messageStorageCache[topic]!!)
                observable.onComplete()
                return@create
            }
            val throwable = AtomicReference<Throwable>()
            val messageStorage = this.messageStorageCache.computeIfAbsent(topic) {
                val ms = MessageStorageImpl(topic, bookKeeper, bookKeeperConfig, metadataStorage, messageOrderedThreadPool)
                ms.initialize()
                        .blockingSubscribe(object : OnCompletedObserver<Void>() {
                            override fun onError(e: Throwable) {
                                throwable.compareAndSet(null, e)
                            }

                            override fun onComplete() {
                            }

                        })
                return@computeIfAbsent ms
            }
            if (throwable.get() != null) {
                observable.onError(throwable.get())
            } else {
                observable.onNext(messageStorage)
                observable.onComplete()
            }
        }
    }

    @Synchronized
    override fun close(name: String) {
       if(!closed){
           this.messageStorageCache.forEach { _, u: MessageStorage -> run { u.close() } }
           this.messageStorageCache.clear()
           this.messageOrderedThreadPool.shutdown()
           if (!this.messageOrderedThreadPool.awaitTermination(60, TimeUnit.SECONDS)) {
               logger.error("Unable to stop message ordered thread pool, in 60S.")
           }
           this.bookKeeper.close()
           this.curatorFramework.close()
           closed = true
       }
    }

    companion object {

        private val logger = LoggerFactory.getLogger(MessageStorageFactoryImpl::class.java)
    }

}
