package com.song.fastmq.storage.storage.impl;

import com.song.fastmq.storage.storage.LogSegmentManager;
import com.song.fastmq.storage.storage.metadata.LogInfo;
import com.song.fastmq.storage.storage.LogManager;
import com.song.fastmq.storage.storage.LogRecord;
import com.song.fastmq.storage.storage.metadata.LogSegmentInfo;
import com.song.fastmq.storage.storage.LogInfoStorage;
import com.song.fastmq.storage.storage.support.LedgerStorageException;
import com.song.fastmq.storage.storage.Position;
import com.song.fastmq.storage.storage.Version;
import com.song.fastmq.storage.storage.concurrent.AsyncCallback;
import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks;
import com.song.fastmq.storage.storage.concurrent.CommonPool;
import com.song.fastmq.storage.storage.config.BookKeeperConfig;
import com.song.fastmq.storage.storage.support.ReadEntryCommand;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.bookie.BookieException.Code;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.commons.collections.CollectionUtils;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by song on 2017/11/5.
 */
public class LogManagerImpl implements LogManager {

    private static final Logger logger = LoggerFactory.getLogger(LogManagerImpl.class);

    private final String name;

    private volatile int ledgerVersion;

    private AtomicInteger lastPosition = new AtomicInteger(-1);

    private volatile LedgerHandle currentLedgerHandle;

    private volatile CompletableFuture<LedgerHandle> currentLedgerHandleFuture;

    private final BookKeeperConfig bookKeeperConfig;

    private final BookKeeper bookKeeper;

    private final AsyncCuratorFramework asyncCuratorFramework;

    private final LogInfoStorage logInfoStorage;

    private final NavigableMap<Long/*Ledger id*/, LogSegmentInfo> ledgers = new ConcurrentSkipListMap<>();

    private final ConcurrentMap<String, LogSegmentManager> cursorCache = new ConcurrentHashMap<>();

    private final Map<Long/*Ledger id*/, CompletableFuture<LedgerHandle>> ledgerCache = new ConcurrentHashMap<>();

    private final AtomicReference<State> state = new AtomicReference<>();

    public LogManagerImpl(String name, BookKeeperConfig config,
        BookKeeper bookKeeper, AsyncCuratorFramework asyncCuratorFramework,
        LogInfoStorage storage) {
        this.name = name;
        bookKeeperConfig = config;
        this.bookKeeper = bookKeeper;
        this.asyncCuratorFramework = asyncCuratorFramework;
        this.logInfoStorage = storage;
        this.state.set(State.NONE);
    }

    public void init(AsyncCallback<Void, LedgerStorageException> asyncCallback) {
        if (state.compareAndSet(State.NONE, State.INITIALIZING)) {
            CommonPool.executeBlocking(() -> logInfoStorage.asyncGetLogInfo(name,
                new AsyncCallback<LogInfo, LedgerStorageException>() {
                    @Override
                    public void onCompleted(LogInfo data, Version version) {
                        ledgerVersion = version.getVersion();
                        if (CollectionUtils.isNotEmpty(data.getLedgers())) {
                            data.getLedgers()
                                .forEach(metadata -> ledgers.put(metadata.getLedgerId(), metadata));
                        }
                        bookKeeper.asyncCreateLedger(bookKeeperConfig.getEnsSize(),
                            bookKeeperConfig.getWriteQuorumSize(),
                            bookKeeperConfig.getDigestType(), bookKeeperConfig.getPassword(),
                            (rc, lh, ctx) -> {
                                if (rc == BookieException.Code.OK) {
                                    long ledgerId = lh.getId();
                                    LogSegmentInfo logSegmentInfo = new LogSegmentInfo();
                                    logSegmentInfo.setLedgerId(ledgerId);
                                    logSegmentInfo.setTimestamp(System.currentTimeMillis());
                                    if (data.getLedgers() == null) {
                                        data.setLedgers(new LinkedList<>());
                                    }
                                    data.getLedgers().add(logSegmentInfo);
                                    logInfoStorage
                                        .asyncUpdateLogInfo(name, data, version,
                                            new AsyncCallback<Void, LedgerStorageException>() {
                                                @Override
                                                public void onCompleted(Void data,
                                                    Version version) {
                                                    ledgerVersion = version.getVersion();
                                                    ledgers.put(ledgerId, logSegmentInfo);
                                                    currentLedgerHandle = lh;
                                                    currentLedgerHandleFuture = new CompletableFuture<>();
                                                    currentLedgerHandleFuture
                                                        .complete(currentLedgerHandle);
                                                    asyncCallback.onCompleted(null, version);
                                                    state.set(State.LEDGER_OPENED);
                                                    logger
                                                        .info("Finish to initialize LogManager");
                                                }

                                                @Override
                                                public void onThrowable(
                                                    LedgerStorageException throwable) {
                                                    asyncCallback.onThrowable(throwable);
                                                }
                                            });
                                } else {
                                    asyncCallback.onThrowable(
                                        new LedgerStorageException(BookieException.create(rc)));
                                }
                            }, null);
                    }

                    @Override
                    public void onThrowable(LedgerStorageException throwable) {
                        asyncCallback.onThrowable(throwable);
                    }
                }));
        } else {
            asyncCallback.onThrowable(new LedgerStorageException("Already initialized!"));
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Position addEntry(byte[] data) throws InterruptedException, LedgerStorageException {
        try {
            checkLedgerManagerIsOpen();
            long entryId = this.currentLedgerHandle.addEntry(data);
            lastPosition.getAndIncrement();
            return new Position(this.currentLedgerHandle.getId(), entryId);
        } catch (Exception e) {
            throw new LedgerStorageException(e);
        }
    }

    @Override
    public void asyncAddEntry(byte[] data,
        AsyncCallback<Position, LedgerStorageException> asyncCallback) {
        try {
            checkLedgerManagerIsOpen();
        } catch (LedgerStorageException e) {
            asyncCallback.onThrowable(e);
            return;
        }
        if (state.get() == State.LEDGER_CLOSING || state.get() == State.LEDGER_CREATING) {
            // TODO: 2017/11/19 queue this request
            asyncCallback
                .onThrowable(new LedgerStorageException("There is no ready ledger to write to!"));
            return;
        }
        this.currentLedgerHandle.asyncAddEntry(data, (rc, lh, entryId, ctx) -> {
            if (rc == BookieException.Code.OK) {
                lastPosition.getAndIncrement();
                asyncCallback.onCompleted(new Position(lh.getId(), entryId), new ZkVersion(0));
            } else {
                asyncCallback.onThrowable(new LedgerStorageException(BookieException.create(rc)));
            }
        }, null);
    }

    @Override
    public void asyncOpenCursor(String name, AsyncCallbacks.OpenCursorCallback callback) {
        try {
            checkLedgerManagerIsOpen();
        } catch (LedgerStorageException e) {
            callback.onThrowable(e);
            return;
        }
        LogSegmentManager logSegmentManager = cursorCache.get(name);
        if (logSegmentManager != null) {
            callback.onComplete(logSegmentManager);
            return;
        }
        logger.debug("Create new cursor :{}. ", name);
        cursorCache.computeIfAbsent(name, s -> {
            LogSegmentManagerImpl ledgerCursorImpl = new LogSegmentManagerImpl(name, this,
                asyncCuratorFramework);
            try {
                ledgerCursorImpl.init();
            } catch (Throwable e) {
                callback.onThrowable(e);
                return null;
            }
            callback.onComplete(ledgerCursorImpl);
            return ledgerCursorImpl;
        });
    }


    void asyncReadEntries(ReadEntryCommand readEntryCommand) {
        // TODO: 2017/11/19 custom thread pool
        Position position = readEntryCommand.getReadPosition();
        int maxNumberToRead = readEntryCommand.getMaxNumberToRead();
        CommonPool.executeBlocking(() -> {
            LogSegmentInfo logSegmentInfo = this.ledgers.get(position.getLedgerId());
            if (logSegmentInfo == null) {
                readEntryCommand.readEntryFailed(
                    new InvalidLedgerException("Ledger id is invalid ,maybe it was deleted."));
                return;
            }
            getLedgerHandle(position.getLedgerId()).thenAccept(ledgerHandle -> {
                long lastAddConfirmed;
                if (ledgerHandle.getId() == currentLedgerHandle.getId()) {
                    lastAddConfirmed = lastPosition.get();
                } else {
                    lastAddConfirmed = ledgerHandle.getLastAddConfirmed();
                }
                long lastEntryId = Math
                    .min(position.getEntryId() + maxNumberToRead - 1, lastAddConfirmed);
                long startEntryId = position.getEntryId();
                if (position.getEntryId() < 0) {
                    startEntryId = 0;
                }
                if (startEntryId > lastEntryId) {
                    //Check if we still have more entries to read from the next ledger
                    long nextValidLedgerId = getNextValidLedgerId(position.getLedgerId());
                    logger.info("Current all entries has been read [{}],move to next one [{}].",
                        position.getLedgerId(), nextValidLedgerId);
                    if (nextValidLedgerId != -1) {
                        readEntryCommand.updateReadPosition(new Position(nextValidLedgerId, 0));
                        CommonPool.executeBlocking(() -> {
                            readEntryCommand
                                .setMaxNumberToRead(readEntryCommand.getMaxNumberToRead());
                            asyncReadEntries(readEntryCommand);
                        });
                    } else {
                        logger.info("All entries of [{}] has been read by [{}] for now.", name,
                            readEntryCommand.name());
                        readEntryCommand.readEntryComplete(Collections.emptyList());
                    }
                    logger.info("All entries from Ledger handle [{}] has been read.",
                        position.getLedgerId());
                    return;
                }
                ledgerHandle.asyncReadEntries(startEntryId, lastEntryId, (rc, lh, seq, ctx) -> {
                    if (rc == Code.OK) {
                        List<LogRecord> ledgerEntries = new ArrayList<>();
                        while (seq.hasMoreElements()) {
                            org.apache.bookkeeper.client.LedgerEntry entry = seq.nextElement();
                            ledgerEntries.add(new LogRecord(entry.getEntry(),
                                new Position(entry.getLedgerId(), entry.getEntryId())));
                        }
                        // TODO: 2017/12/4 Move to next ledger if not able to read enough entries.
                        if (ledgerEntries.size() < readEntryCommand.getMaxNumberToRead()) {
                            LogRecord logRecord = ledgerEntries.get(ledgerEntries.size() - 1);
                            readEntryCommand.updateReadPosition(
                                new Position(logRecord.getPosition().getLedgerId() + 1,
                                    logRecord.getPosition().getEntryId()));
                            readEntryCommand.setMaxNumberToRead(
                                readEntryCommand.getMaxNumberToRead() - ledgerEntries.size());
                            logger.info("Trying to read more from next ledger,{}.",
                                logRecord.getPosition().toString());
                            readEntryCommand.addEntries(ledgerEntries);
                            CommonPool.executeBlocking(() -> asyncReadEntries(readEntryCommand));
                        } else {
                            readEntryCommand.readEntryComplete(ledgerEntries);
                        }
                    } else {
                        readEntryCommand
                            .readEntryFailed(new LedgerStorageException(BKException.create(rc)));
                    }
                }, null);
            }).exceptionally(throwable -> {
                readEntryCommand.readEntryFailed(new LedgerStorageException(throwable));
                return null;
            });
        });
    }

    CompletableFuture<LedgerHandle> getLedgerHandle(long ledgerId) {
        if (this.currentLedgerHandle.getId() == ledgerId) {
            return this.currentLedgerHandleFuture;
        }
        CompletableFuture<LedgerHandle> completableFuture = this.ledgerCache.get(ledgerId);
        if (completableFuture != null) {
            return completableFuture;
        }
        return this.ledgerCache.computeIfAbsent(ledgerId, id -> {
            CompletableFuture<LedgerHandle> future = new CompletableFuture<>();
            logger.info("Try to open ledger:{}", ledgerId);
            try {
                LedgerHandle ledgerHandle = this.bookKeeper
                    .openLedger(ledgerId, this.bookKeeperConfig.getDigestType(),
                        this.bookKeeperConfig.getPassword());
                this.bookKeeper.asyncOpenLedger(ledgerId, this.bookKeeperConfig.getDigestType(),
                    this.bookKeeperConfig.getPassword(), (rc, lh, ctx) -> {
                        if (rc != BKException.Code.OK) {
                            // Remove from cache to let another thread reopen it
                            ledgerCache.remove(ledgerId, future);
                            future.completeExceptionally(
                                new LedgerStorageException(BKException.getMessage(rc)));
                        } else {
                            logger.debug("[{}] Successfully opened ledger {} for reading", name,
                                lh.getId());
                            future.complete(lh);
                        }
                    }, null);
                future.complete(ledgerHandle);
                logger.info("Open ledger[{}] done", ledgerId);
            } catch (BKException | InterruptedException e) {
                future.completeExceptionally(e);
            }
            return future;
        });
    }

    long getNextValidLedgerId(long ledgerId) {
        Long nextValidLedgerId = this.ledgers.higherKey(ledgerId);
        return nextValidLedgerId != null ? nextValidLedgerId : -1;
    }

    @Override
    public void close() throws InterruptedException, LedgerStorageException {
        if (state.get() == State.CLOSED) {
            logger.warn("LogManager is closed,so we just ignore this close quest.");
            return;
        }
        try {
            if (this.currentLedgerHandle != null) {
                this.currentLedgerHandle.close();
            }
            state.set(State.CLOSED);
        } catch (BKException e) {
            throw new LedgerStorageException(e);
        }
    }

    private void checkLedgerManagerIsOpen() throws LedgerStorageException {
        if (state.get() == State.CLOSED) {
            throw new LedgerStorageException("LogManager " + name + " has already been closed");
        }
    }

    public LedgerHandle getCurrentLedgerHandle() {
        return currentLedgerHandle;
    }

    public LogInfoStorage getLogInfoStorage() {
        return logInfoStorage;
    }

    enum State {
        NONE,
        INITIALIZING,
        LEDGER_OPENED,
        LEDGER_CLOSING,
        LEDGER_CLOSED,
        LEDGER_CREATING,
        CLOSED,
        FENCED,
    }
}
