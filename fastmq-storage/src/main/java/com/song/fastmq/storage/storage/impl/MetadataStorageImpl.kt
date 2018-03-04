package com.song.fastmq.storage.storage.impl

import com.fasterxml.jackson.core.JsonProcessingException
import com.song.fastmq.common.utils.JsonUtils
import com.song.fastmq.storage.storage.MetadataStorage
import com.song.fastmq.storage.storage.metadata.Log
import io.reactivex.Observable
import io.reactivex.ObservableEmitter
import org.apache.curator.x.async.AsyncCuratorFramework
import org.apache.curator.x.async.api.CreateOption
import org.apache.curator.x.async.api.DeleteOption
import org.slf4j.LoggerFactory
import java.util.*

/**
 * Created by song on 2017/11/5.
 */
class MetadataStorageImpl(private val asyncCuratorFramework: AsyncCuratorFramework) : MetadataStorage {

    override fun getLogInfo(name: String): Observable<Log> {
        return Observable.create<Log> { observable: ObservableEmitter<Log> ->
            val ledgerManagerPath = LEDGER_NAME_PREFIX + name
            this.asyncCuratorFramework.checkExists().forPath(ledgerManagerPath).whenComplete({ stat, throwable ->
                if (throwable != null) {
                    observable.onError(throwable)
                    return@whenComplete
                }
                if (stat == null) {
                    try {
                        val log = Log(name)
                        val bytes = JsonUtils.toJson(log).toByteArray()
                        this.asyncCuratorFramework.create().withOptions(EnumSet.of(CreateOption.createParentsIfNeeded)).forPath(ledgerManagerPath, bytes)
                                .whenComplete({ _, t ->
                                    if (t != null) {
                                        observable.onError(t)
                                    } else {
                                        observable.onNext(log)
                                        observable.onComplete()
                                    }
                                })
                    } catch (e: JsonUtils.JsonException) {
                        observable.onError(e)
                        return@whenComplete
                    }
                } else {
                    this.asyncCuratorFramework.data.forPath(ledgerManagerPath).whenComplete({ bytes, t ->
                        if (t != null) {
                            observable.onError(t)
                        } else {
                            try {
                                observable.onNext(JsonUtils.fromJson(String(bytes), Log::class.java))
                                observable.onComplete()
                            } catch (e: JsonUtils.JsonException) {
                                observable.onError(e)
                            }
                        }
                    })
                }
            })
        }
    }

    override fun updateLogInfo(name: String, log: Log): Observable<Void> {
        return Observable.create { observable: ObservableEmitter<Void> ->
            try {
                val bytes = JsonUtils.get().writeValueAsBytes(log)
                this.asyncCuratorFramework
                        .setData()
                        .forPath(LEDGER_NAME_PREFIX + name, bytes)
                        .whenComplete { _, throwable ->
                            if (throwable != null) {
                                observable.onError(throwable)
                            } else {
                                observable.onComplete()
                            }
                            return@whenComplete
                        }
            } catch (e: JsonProcessingException) {
                observable.onError(e)
                return@create
            }
        }
    }

    override fun removeLogInfo(name: String): Observable<Void> {
        logger.info("Remove ledger [{}].", name)
        return Observable.create {
            this.asyncCuratorFramework
                    .delete()
                    .withOptions(EnumSet.of(DeleteOption.guaranteed))
                    .forPath(LEDGER_NAME_PREFIX + name)
                    .whenComplete { _, throwable ->
                        if (throwable != null) {
                            it.onError(throwable)
                        } else {
                            it.onComplete()
                        }
                    }
        }
    }

    companion object {

        private val logger = LoggerFactory.getLogger(MetadataStorageImpl::class.java)

        private val LEDGER_NAME_PREFIX_NAME = "/fastmq/bk-ledgers"

        private val LEDGER_NAME_PREFIX = LEDGER_NAME_PREFIX_NAME + "/"
    }
}
