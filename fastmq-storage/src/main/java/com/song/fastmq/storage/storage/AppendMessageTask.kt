package com.song.fastmq.storage.storage

import com.google.common.base.Preconditions.checkArgument
import com.song.fastmq.storage.storage.impl.MessageStorageImpl
import com.song.fastmq.storage.storage.support.MessageStorageException
import io.netty.buffer.ByteBuf
import io.reactivex.ObservableEmitter
import org.apache.bookkeeper.client.AsyncCallback
import org.apache.bookkeeper.client.BKException
import org.apache.bookkeeper.client.LedgerHandle
import org.apache.bookkeeper.common.util.SafeRunnable
import org.slf4j.LoggerFactory

/**
 * @author song
 */
class AppendMessageTask(private val messageStorage: MessageStorageImpl, private val data: ByteBuf,
                        private val observable: ObservableEmitter<Offset>) : SafeRunnable, AsyncCallback.AddCallback {

    private var entryId: Long = 0

    private var startTime: Long = 0

    private var dataLength: Int = 0

    var ledgerHandle: LedgerHandle = messageStorage.currentLedger

    init {
        dataLength = data.readableBytes()
        startTime = System.nanoTime()
    }

    fun start() {
        logger.debug("[{}] Async add pending entry", this.ledgerHandle.id)
        this.ledgerHandle.asyncAddEntry(this.data, this, null)
    }

    fun failed(t: Throwable) {
        this.data.release()
        this.observable.onError(t)
    }

    override fun addComplete(rc: Int, lh: LedgerHandle, entryId: Long, ctx: Any?) {
        checkArgument(this.ledgerHandle.id == lh.id)
        this.entryId = entryId
        logger.debug("[{}] write-complete: ledger-id={} entry-id={} size={} rc={}", this.messageStorage.topic,
                lh.id, entryId, dataLength, rc)
        if (rc != BKException.Code.OK) {
            this.observable.onError(MessageStorageException(BKException.create(rc)))
            this.messageStorage.executor.submitOrdered(this.messageStorage.topic, SafeRunnable.safeRun {
                //force to create a new ledger in the background thread
                this.messageStorage.ledgerClosed(lh)
            })
        } else {
            this.messageStorage.executor.submitOrdered(this.messageStorage.topic, this)
        }
    }

    override fun safeRun() {
        this.messageStorage.numberOfEntries.incrementAndGet()
        this.messageStorage.totalSize.addAndGet(dataLength.toLong())

        this.data.release()
        this.messageStorage.lastConfirmedEntry = Offset(this.ledgerHandle.id, this.entryId)
        observable.onNext(Offset(ledgerHandle.id, entryId))
        this.observable.onComplete()
    }

    companion object {
        private val logger = LoggerFactory.getLogger(AppendMessageTask::class.java)
    }
}