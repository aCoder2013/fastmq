package com.song.fastmq.storage.storage.impl

import com.google.common.base.Preconditions.checkArgument
import com.google.common.collect.Lists
import com.google.common.collect.Queues
import com.song.fastmq.common.message.Message
import com.song.fastmq.common.message.MessageId
import com.song.fastmq.common.utils.OnCompletedObserver
import com.song.fastmq.storage.storage.*
import com.song.fastmq.storage.storage.config.BookKeeperConfig
import com.song.fastmq.storage.storage.metadata.Log
import com.song.fastmq.storage.storage.metadata.LogSegment
import com.song.fastmq.storage.storage.support.LedgerClosedException
import com.song.fastmq.storage.storage.support.LedgerStorageException
import com.song.fastmq.storage.storage.support.MessageStorageException
import com.song.fastmq.storage.storage.support.NoMoreMessageException
import io.netty.buffer.Unpooled
import io.reactivex.Observable
import io.reactivex.ObservableEmitter
import org.apache.bookkeeper.client.AsyncCallback
import org.apache.bookkeeper.client.BKException
import org.apache.bookkeeper.client.BookKeeper
import org.apache.bookkeeper.client.LedgerHandle
import org.apache.bookkeeper.util.OrderedSafeExecutor
import org.apache.bookkeeper.util.SafeRunnable.safeRun
import org.apache.zookeeper.KeeperException
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
 * Created by song on 2017/11/5.
 */
class MessageStorageImpl(val topic: String, private val bookKeeper: BookKeeper, private val config: BookKeeperConfig,
                         private val metadataStorage: MetadataStorage, val executor: OrderedSafeExecutor) : MessageStorage, AsyncCallback.CreateCallback {


    val state = AtomicReference<State>()

    private var isClosed: Boolean = false

    private val ledgers = ConcurrentSkipListMap<Long, LogSegment>()

    private val ledgerCache = ConcurrentHashMap<Long, Observable<LedgerHandle>>()

    var numberOfMessages = AtomicLong(0)

    var totalSize = AtomicLong(0)

    @Volatile
    lateinit var currentLedger: LedgerHandle

    private var lastLedgerCreationInitiationTimestamp: Long = 0

    private var lastLedgerCreatedTimestamp: Long = 0

    private var lastLedgerCreationFailureTimestamp: Long = 0

    @Volatile
    lateinit var lastConfirmedEntry: Offset

    private val pendingAppendMessageQueue = Queues.newConcurrentLinkedQueue<AppendMessageTask>()

    init {
        this.state.set(State.NONE)
    }

    @Synchronized
    @Throws(MessageStorageException::class)
    fun initialize(): Observable<Void> {
        return Observable.create<Void> { observable: ObservableEmitter<Void> ->
            logger.info("Open message storage {}.", this.topic)
            this.metadataStorage.getLogInfo(this.topic).subscribe {
                it.segments.forEach {
                    this.ledgers.put(it.ledgerId, it)
                }
                if (this.ledgers.size > 0) {
                    val id = ledgers.lastKey().toLong()
                    this.bookKeeper.asyncOpenLedger(id, config.digestType, config.password, { rc, lh, _ ->
                        logger.debug("[{}] Opened ledger {}: ", this.topic, id, BKException.getMessage(rc))
                        when (rc) {
                            BKException.Code.OK -> {
                                val logSegment = LogSegment()
                                logSegment.ledgerId = id
                                logSegment.entries = lh.lastAddConfirmed + 1
                                logSegment.size = lh.length
                                logSegment.timestamp = System.currentTimeMillis()
                                ledgers.put(id, logSegment)
                                initializeBookKeeper().subscribe(object : OnCompletedObserver<Void>() {
                                    override fun onError(e: Throwable) {
                                        observable.onError(e)
                                    }

                                    override fun onComplete() {
                                        observable.onComplete()
                                    }

                                })
                                return@asyncOpenLedger
                            }
                            BKException.Code.NoSuchLedgerExistsException -> {
                                logger.warn("[{}] Ledger not found: {}", this.topic, ledgers.lastKey())
                                ledgers.remove(ledgers.lastKey())
                                initializeBookKeeper().subscribe(object : OnCompletedObserver<Void>() {
                                    override fun onError(e: Throwable) {
                                        observable.onError(e)
                                    }

                                    override fun onComplete() {
                                        observable.onComplete()
                                    }
                                })
                                return@asyncOpenLedger
                            }
                            else -> {
                                logger.error("[{}] Failed to open ledger {}: {}", this.topic, id, BKException.getMessage(rc))
                                observable.onError(MessageStorageException(BKException.getMessage(rc)))
                                return@asyncOpenLedger
                            }
                        }
                    }, null)
                } else {
                    initializeBookKeeper().subscribe({
                        observable.onComplete()
                    }, {
                        observable.onError(it)
                    })
                }
            }
        }
    }

    private fun initializeBookKeeper(): Observable<Void> {
        return Observable.create<Void> { observable: ObservableEmitter<Void> ->
            logger.debug("[{}] initializing bookkeeper; ledgers {}", this.topic, ledgers)

            // Calculate total entries and size
            val iterator = ledgers.values.iterator()
            while (iterator.hasNext()) {
                val logSegment = iterator.next()
                if (logSegment.entries > 0) {
                    numberOfMessages.addAndGet(logSegment.entries)
                    totalSize.addAndGet(logSegment.size)
                } else {
                    iterator.remove()
                    bookKeeper.asyncDeleteLedger(logSegment.ledgerId, { rc, _ ->
                        logger.info("[{}] Deleted empty ledger ledgerId={} rc={}", this.topic, logSegment.ledgerId, rc)
                    }, null)
                }
            }

            // Create a new ledger to start writing
            this.lastLedgerCreationInitiationTimestamp = System.nanoTime()
            this.bookKeeper.asyncCreateLedger(config.ensSize, config.writeQuorumSize, config.ackQuorumSize,
                    config.digestType, config.password, { rc, lh, _ ->

                //don't block bk thread
                this.executor.submitOrdered(this.topic, safeRun({
                    if (rc != BKException.Code.OK) {
                        observable.onError(MessageStorageException(org.apache.bookkeeper.client.BKException.getMessage(rc)))
                        return@safeRun
                    }

                    logger.info("[{}] Created ledger {}", this.topic, lh.id)
                    state.set(State.LEDGER_OPENED)
                    lastLedgerCreatedTimestamp = System.currentTimeMillis()
                    currentLedger = lh
                    lastConfirmedEntry = Offset(lh.id, -1)
                    this.ledgers.put(lh.id, LogSegment(lh.id))

                    // Save it back to ensure all nodes exist
                    val log = Log()
                    log.segments = ArrayList(this.ledgers.values)
                    this.metadataStorage.updateLogInfo(this.topic, log).subscribe(object : OnCompletedObserver<Void>() {
                        override fun onComplete() {
                            observable.onComplete()
                        }

                        override fun onError(e: Throwable) {
                            observable.onError(e)
                        }
                    })
                }))
            }, null, null)
        }
    }

    override fun appendMessage(message: Message): Observable<Offset> {
        return Observable.create<Offset> { observable: ObservableEmitter<Offset> ->
            this.executor.submitOrdered(this.topic, safeRun {
                val buffer = Unpooled.wrappedBuffer(message.data)
                logger.debug("[{}] asyncAddEntry size={} state={}", this.topic, buffer.readableBytes(), state)
                val state = this.state.get()
                if (state == State.Fenced) {
                    observable.onError(MessageStorageException("Attempted to use a fenced managed ledger"))
                    return@safeRun
                } else if (state == State.CLOSED) {
                    observable.onError(MessageStorageException("Message storage was already closed"))
                    return@safeRun
                }
                if (state == State.LEDGER_CLOSING || state == State.LEDGER_CREATING) {
                    logger.debug("[{}] Queue addEntry request", this.topic)
                    this.pendingAppendMessageQueue.offer(AppendMessageTask(this, buffer, observable))
                } else if (state == State.LEDGER_CLOSED) {
                    val now = System.currentTimeMillis()
                    if (now < lastLedgerCreationFailureTimestamp + WAIT_TIME_AFTER_LEDGER_CREATION_FAILURE_MS) {
                        observable.onError(MessageStorageException("Waiting for new ledger creation to complete"))
                        return@safeRun
                    }
                    logger.info("Create a new ledger for {}.", this.topic)
                    if (this.state.compareAndSet(State.LEDGER_CLOSED, State.LEDGER_CREATING)) {
                        this.lastLedgerCreationInitiationTimestamp = System.nanoTime()
                        this.pendingAppendMessageQueue.offer(AppendMessageTask(this, buffer, observable))
                        this.bookKeeper.asyncCreateLedger(config.ensSize, config.writeQuorumSize, config.ackQuorumSize, config.digestType,
                                config.password, this, null, null)
                    } else {
                        this.pendingAppendMessageQueue.offer(AppendMessageTask(this, buffer, observable))
                    }
                } else {
                    checkArgument(state == State.LEDGER_OPENED)
                    logger.debug("[{}] Write into current ledger lh={}", this.topic, currentLedger.id)
                    val task = AppendMessageTask(this, buffer, observable)
                    task.ledgerHandle = this.currentLedger
                    task.start()
                }
            })
        }
    }

    override fun queryMessage(offset: Offset, maxMsgNum: Int): Observable<BatchMessage> {
        return Observable.create<BatchMessage> { observable: ObservableEmitter<BatchMessage> ->
            this.executor.submitOrdered(this.topic, safeRun {
                checkArgument(maxMsgNum > 0)
                val state = state.get()
                if (state == State.Fenced || state == State.CLOSED) {
                    observable.onError(LedgerClosedException("Attempted to use a fenced of closed managed ledger"))
                    return@safeRun
                }
                val ledgerId = offset.ledgerId
                val logSegment = this.ledgers[ledgerId]
                if (logSegment == null) {
                    observable.onError(MessageStorageException("$topic Ledger[$ledgerId] didn't exist."))
                    return@safeRun
                }
                getLedgerHandle(ledgerId).subscribe(object : OnCompletedObserver<LedgerHandle>() {

                    override fun onNext(t: LedgerHandle) {

                        val firstEntry = offset.entryId

                        val lastPosition = lastConfirmedEntry

                        val lastEntryInLedger = if (lastPosition.ledgerId == t.id) {
                            lastPosition.entryId
                        } else {
                            t.lastAddConfirmed
                        }

                        if (firstEntry > lastEntryInLedger) {
                            logger.info("[{}] No more message to read from ledger = {} lastEntry = {} ,try to move to next one.", topic, ledgerId, lastEntryInLedger)
                            if (ledgerId != currentLedger.id) {
                                val nextLedgerId = ledgers.ceilingKey(ledgerId + 1)
                                if (nextLedgerId == null) {
                                    observable.onNext(BatchMessage(messages = Collections.emptyList()))
                                    observable.onComplete()
                                    return
                                } else {
                                    executor.submitOrdered(nextLedgerId, safeRun {
                                        queryMessage(Offset(nextLedgerId, 0), maxMsgNum)
                                                .subscribe(object : OnCompletedObserver<BatchMessage>() {

                                                    override fun onError(e: Throwable) {
                                                        observable.onError(e)
                                                    }

                                                    override fun onNext(t: BatchMessage) {
                                                        observable.onNext(t)
                                                    }

                                                    override fun onComplete() {
                                                        observable.onComplete()
                                                    }

                                                })
                                    })
                                }
                            } else {
                                logger.info("[{}] No more message to read from current ledger [{}].", topic, ledgerId)
                                observable.onError(NoMoreMessageException("No more message to read from current ledger :" + ledgerId))
                            }
                            return
                        }
                        val lastEntry = Math.min(firstEntry + maxMsgNum - 1, lastEntryInLedger)
                        logger.debug("[{}] Reading entries from ledger {} - first={} last={}", this@MessageStorageImpl.topic, t.id, firstEntry, lastEntry)
                        t.asyncReadEntries(firstEntry, lastEntry, { rc, _, seq, _ ->
                            if (rc != BKException.Code.OK) {
                                observable.onError(MessageStorageException(BKException.create(rc)))
                                return@asyncReadEntries
                            } else {
                                var totalSize: Long = 0
                                val messages = Lists.newArrayListWithExpectedSize<Message>(maxMsgNum)
                                while (seq.hasMoreElements()) {
                                    val ledgerEntry = seq.nextElement()
                                    totalSize += ledgerEntry.length
                                    val entryBuffer = ledgerEntry.entryBuffer
                                    val array = ByteArray(entryBuffer.readableBytes())
                                    entryBuffer.getBytes(entryBuffer.readerIndex(), array)
                                    val message = Message(messageId = MessageId(ledgerEntry.ledgerId, ledgerEntry.entryId), data = array)
                                    messages.add(message)
                                }
                                observable.onNext(BatchMessage(Offset(ledgerId, messages[messages.size - 1].messageId.entryId + 1), messages))
                                observable.onComplete()
                                return@asyncReadEntries
                            }
                        }, null)
                    }

                    override fun onComplete() {
                    }

                    override fun onError(e: Throwable) {
                        logger.error("Error open ledger handle [{}],read offset {} - {}", ledgerId, offset, e.message)
                        observable.onError(e)
                    }
                })
            })
        }
    }

    private fun getLedgerHandle(ledgerId: Long): Observable<LedgerHandle> {
        if (this.currentLedger.id == ledgerId) {
            return Observable.create<LedgerHandle> {
                it.onNext(this.currentLedger)
                it.onComplete()
            }
        }
        val ledgerHandle = this.ledgerCache[ledgerId]
        if (ledgerHandle != null) {
            return ledgerHandle
        }
        return this.ledgerCache.computeIfAbsent(ledgerId, { lld: Long ->
            Observable.create<LedgerHandle> {
                this.bookKeeper.asyncOpenLedgerNoRecovery(lld, this.config.digestType, this.config.password, { rc, lh, _ ->
                    this.executor.submitOrdered(ledgerId, safeRun {
                        if (rc != BKException.Code.OK) {
                            ledgerCache.remove(ledgerId)
                            it.onError(MessageStorageException(BKException.getMessage(rc)))
                            return@safeRun
                        } else {
                            it.onNext(lh)
                            it.onComplete()
                            return@safeRun
                        }
                    })
                }, null)
            }
        })
    }

    private fun internalReadFromLedger(ledgerHandle: LedgerHandle) {

    }

    override fun getNumberOfMessages(): Long {
        return this.numberOfMessages.get()
    }

    @Synchronized
    @Throws(InterruptedException::class, LedgerStorageException::class)
    override fun close() {
        if (this.isClosed) {
            logger.warn("Message storage[{}] is already closed.", this.topic)
        } else {
            this.currentLedger.close()
            this.isClosed = true
            this.state.set(State.CLOSED)
            logger.info("Message storage[{}] is closed.", this.topic)
        }
    }

    override fun createComplete(rc: Int, lh: LedgerHandle, ctx: Any?) {
        logger.debug("[{}] createComplete rc={} ledger={}", this.topic, rc, lh.id)
        if (rc != BKException.Code.OK) {
            logger.error("[{}] Error creating ledger rc={} {}", this.topic, rc, BKException.getMessage(rc))
            lastLedgerCreationFailureTimestamp = System.currentTimeMillis()
            this.state.set(State.LEDGER_CLOSED)
        } else {
            logger.info("[{}] Created new ledger {}", this.topic, lh.id)
            ledgers.put(lh.id, LogSegment(lh.id))
            currentLedger = lh

            updateLogInfo().subscribe({
                synchronized(this.pendingAppendMessageQueue) {
                    this.pendingAppendMessageQueue.forEach({ task: AppendMessageTask ->
                        task.ledgerHandle = this.currentLedger
                        task.start()
                    })
                    this.pendingAppendMessageQueue.clear()
                }
                this.state.set(State.LEDGER_OPENED)
                lastLedgerCreatedTimestamp = System.currentTimeMillis()
            }, { throwable ->
                if (throwable is KeeperException.BadVersionException) {
                    synchronized(MessageStorageImpl::class) {
                        this.pendingAppendMessageQueue.forEach {
                            it.failed(throwable)
                        }
                        logger.error("[{}] Failed to update log metadata , z-node version mismatch. Closing message storage", this.topic)
                        this.state.set(State.Fenced)
                        return@subscribe
                    }
                } else {
                    logger.warn("[{}] Error updating meta data with the new list of ledgers: {}", this.topic, throwable.message)
                    ledgers.remove(lh.id)
                    bookKeeper.asyncDeleteLedger(lh.id, { rc1, _ ->
                        if (rc1 != BKException.Code.OK) {
                            logger.warn("[{}] Failed to delete ledger {}: {}", this.topic, lh.id,
                                    BKException.getMessage(rc1))
                        }
                    }, null)
                    synchronized(MessageStorageImpl::class) {
                        this.state.set(State.LEDGER_CLOSED)
                        lastLedgerCreationFailureTimestamp = System.currentTimeMillis()
                    }
                }
            })
        }
    }

    @Synchronized
    private fun updateLogInfo(): Observable<Void> {
        return Observable.create { observable: ObservableEmitter<Void> ->
            val log = Log(this.topic)
            log.segments = ArrayList(ledgers.values)
            this.metadataStorage.updateLogInfo(this.topic, log).subscribe(object : OnCompletedObserver<Void>() {
                override fun onError(e: Throwable) {
                    observable.onError(e)
                }

                override fun onComplete() {
                    observable.onComplete()
                }

            })
        }
    }

    @Synchronized
    fun ledgerClosed(lh: LedgerHandle) {
        val state = this.state.get()
        if (state === State.LEDGER_CLOSING || state === State.LEDGER_OPENED) {
            this.state.set(State.LEDGER_CLOSED)
        } else {
            return
        }

        val entriesInLedger = lh.lastAddConfirmed + 1
        logger.debug("[{}] Ledger has been closed id={} entries={}", this.topic, lh.id, entriesInLedger)
        if (entriesInLedger > 0) {
            val logSegment = LogSegment()
            logSegment.ledgerId = lh.id
            logSegment.entries = entriesInLedger
            logSegment.size = lh.length
            logSegment.timestamp = System.currentTimeMillis()
            this.ledgers.put(lh.id, logSegment)
        } else {
            this.ledgers.remove(lh.id)
            this.bookKeeper.asyncDeleteLedger(lh.id, { rc, _ ->
                logger.info("[{}] Delete empty ledger {}. rc={}", this.topic, lh.id, rc)
            }, null)
        }
    }

    enum class State {
        NONE,// Uninitialized
        INITIALIZING,
        LEDGER_OPENED, // Ready to write into
        LEDGER_CLOSING,
        LEDGER_CLOSED,
        LEDGER_CREATING,
        CLOSED,
        Fenced, // A ledger is fenced when there is some concurrent
        // access from a different session/machine. In this state the
        // managed ledger will throw exception for all operations, since
        // the new instance will take over
    }

    companion object {

        private val logger = LoggerFactory.getLogger(MessageStorageImpl::class.java)

        // Time period in which new write requests will not be accepted, after we fail in creating a new ledger.
        private val WAIT_TIME_AFTER_LEDGER_CREATION_FAILURE_MS: Long = 10000
    }
}
