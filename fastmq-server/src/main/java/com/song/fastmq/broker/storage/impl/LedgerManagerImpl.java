package com.song.fastmq.broker.storage.impl;

import com.song.fastmq.broker.storage.LedgerCursor;
import com.song.fastmq.broker.storage.LedgerEntryWrapper;
import com.song.fastmq.broker.storage.LedgerInfo;
import com.song.fastmq.broker.storage.LedgerInfoManager;
import com.song.fastmq.broker.storage.LedgerManager;
import com.song.fastmq.broker.storage.LedgerManagerStorage;
import com.song.fastmq.broker.storage.LedgerStorageException;
import com.song.fastmq.broker.storage.Position;
import com.song.fastmq.broker.storage.Version;
import com.song.fastmq.broker.storage.concurrent.AsyncCallback;
import com.song.fastmq.broker.storage.concurrent.AsyncCallbacks;
import com.song.fastmq.broker.storage.concurrent.CommonPool;
import com.song.fastmq.broker.storage.config.BookKeeperConfig;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.bookie.BookieException;
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
public class LedgerManagerImpl implements LedgerManager {

    private static final Logger logger = LoggerFactory.getLogger(LedgerManagerImpl.class);

    private final String name;

    private volatile int ledgerVersion;

    private AtomicInteger lastPosition = new AtomicInteger(-1);

    private volatile LedgerHandle currentLedgerHandle;

    private volatile CompletableFuture<LedgerHandle> currentLedgerHandleFuture;

    private final BookKeeperConfig bookKeeperConfig;

    private final BookKeeper bookKeeper;

    private final AsyncCuratorFramework asyncCuratorFramework;

    private final LedgerManagerStorage ledgerManagerStorage;

    private final NavigableMap<Long/*Ledger id*/, LedgerInfo> ledgers = new ConcurrentSkipListMap<>();

    private final ConcurrentMap<String, LedgerCursor> cursorCache = new ConcurrentHashMap<>();

    private final Map<Long/*Ledger id*/, CompletableFuture<LedgerHandle>> ledgerCache = new ConcurrentHashMap<>();

    private final AtomicReference<State> state = new AtomicReference<>();

    public LedgerManagerImpl(String name, BookKeeperConfig config,
        BookKeeper bookKeeper, AsyncCuratorFramework asyncCuratorFramework, LedgerManagerStorage storage) {
        this.name = name;
        bookKeeperConfig = config;
        this.bookKeeper = bookKeeper;
        this.asyncCuratorFramework = asyncCuratorFramework;
        this.ledgerManagerStorage = storage;
        this.state.set(State.NONE);
    }

    public void init(AsyncCallback<Void, LedgerStorageException> asyncCallback) {
        if (state.compareAndSet(State.NONE, State.INITIALIZING)) {
            CommonPool.executeBlocking(() -> ledgerManagerStorage.asyncGetLedgerManager(name, new AsyncCallback<LedgerInfoManager, LedgerStorageException>() {
                @Override public void onCompleted(LedgerInfoManager data, Version version) {
                    ledgerVersion = version.getVersion();
                    if (CollectionUtils.isNotEmpty(data.getLedgers())) {
                        data.getLedgers().forEach(metadata -> ledgers.put(metadata.getLedgerId(), metadata));
                    }
                    bookKeeper.asyncCreateLedger(bookKeeperConfig.getEnsSize(), bookKeeperConfig.getWriteQuorumSize(),
                        bookKeeperConfig.getDigestType(), bookKeeperConfig.getPassword(), (rc, lh, ctx) -> {
                            if (rc == BookieException.Code.OK) {
                                long ledgerId = lh.getId();
                                LedgerInfo ledgerInfo = new LedgerInfo();
                                ledgerInfo.setLedgerId(ledgerId);
                                ledgerInfo.setTimestamp(System.currentTimeMillis());
                                if (data.getLedgers() == null) {
                                    data.setLedgers(new LinkedList<>());
                                }
                                data.getLedgers().add(ledgerInfo);
                                ledgerManagerStorage.asyncUpdateLedgerManager(name, data, version, new AsyncCallback<Void, LedgerStorageException>() {
                                    @Override public void onCompleted(Void data, Version version) {
                                        ledgerVersion = version.getVersion();
                                        ledgers.put(ledgerId, ledgerInfo);
                                        currentLedgerHandle = lh;
                                        currentLedgerHandleFuture = new CompletableFuture<>();
                                        currentLedgerHandleFuture.complete(currentLedgerHandle);
                                        asyncCallback.onCompleted(null, version);
                                        state.set(State.LEDGER_OPENED);
                                        logger.info("Finish to initialize LedgerManager");
                                    }

                                    @Override public void onThrowable(LedgerStorageException throwable) {
                                        asyncCallback.onThrowable(throwable);
                                    }
                                });
                            } else {
                                asyncCallback.onThrowable(new LedgerStorageException(BookieException.create(rc)));
                            }
                        }, null);
                }

                @Override public void onThrowable(LedgerStorageException throwable) {
                    asyncCallback.onThrowable(throwable);
                }
            }));
        } else {
            asyncCallback.onThrowable(new LedgerStorageException("Already initialized!"));
        }
    }

    @Override public String getName() {
        return name;
    }

    @Override public Position addEntry(byte[] data) throws InterruptedException, LedgerStorageException {
        try {
            checkLedgerManagerIsOpen();
            long entryId = this.currentLedgerHandle.addEntry(data);
            lastPosition.getAndIncrement();
            return new Position(this.currentLedgerHandle.getId(), entryId);
        } catch (Exception e) {
            throw new LedgerStorageException(e);
        }
    }

    @Override public void asyncAddEntry(byte[] data, AsyncCallback<Position, LedgerStorageException> asyncCallback) {
        try {
            checkLedgerManagerIsOpen();
        } catch (LedgerStorageException e) {
            asyncCallback.onThrowable(e);
            return;
        }
        if (state.get() == State.LEDGER_CLOSING || state.get() == State.LEDGER_CREATING) {
            // TODO: 2017/11/19 queue this request
            asyncCallback.onThrowable(new LedgerStorageException("There is no ready ledger to write to!"));
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

    @Override public void asyncOpenCursor(String name, AsyncCallbacks.OpenCursorCallback callback) {
        try {
            checkLedgerManagerIsOpen();
        } catch (LedgerStorageException e) {
            callback.onThrowable(e);
            return;
        }
        LedgerCursor ledgerCursor = cursorCache.get(name);
        if (ledgerCursor != null) {
            callback.onComplete(ledgerCursor);
            return;
        }
        logger.debug("Create new cursor :{}. ", name);
        cursorCache.computeIfAbsent(name, s -> {
            LedgerCursorImpl ledgerCursorImpl = new LedgerCursorImpl(name, this, asyncCuratorFramework);
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

    List<LedgerEntryWrapper> readEntries(int numberToRead,
        Position position) throws InterruptedException, LedgerStorageException {
        CompletableFuture<List<LedgerEntryWrapper>> future = new CompletableFuture<>();
        asyncReadEntries(numberToRead, position, new AsyncCallbacks.ReadEntryCallback() {
            @Override public void readEntryComplete(List<LedgerEntryWrapper> entries) {
                future.complete(entries);
            }

            @Override public void readEntryFailed(Throwable throwable) {
                future.completeExceptionally(throwable);
            }
        });
        try {
            // TODO: 2017/11/19 timeout
            return future.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof InterruptedException) {
                throw (InterruptedException) e.getCause();
            }
            throw new LedgerStorageException(e.getCause());
        }
    }

    void asyncReadEntries(int numberToRead, Position position,
        AsyncCallbacks.ReadEntryCallback callback) {
        // TODO: 2017/11/19 custom thread pool
        CommonPool.executeBlocking(() -> {
            LedgerInfo ledgerInfo = this.ledgers.get(position.getLedgerId());
            if (ledgerInfo == null) {
                callback.readEntryFailed(new InvalidLedgerException("Ledger id is invalid ,maybe it was deleted."));
                return;
            }
            getLedgerHandle(position.getLedgerId()).thenAccept(ledgerHandle -> {
                long lastAddConfirmed;
                if (ledgerHandle.getId() == currentLedgerHandle.getId()) {
                    lastAddConfirmed = lastPosition.get();
                } else {
                    lastAddConfirmed = ledgerHandle.getLastAddConfirmed();
                }
                long lastEntryId = Math.min(position.getEntryId() + numberToRead - 1, lastAddConfirmed);
                long startEntryId = position.getEntryId();
                if (position.getEntryId() < 0) {
                    startEntryId = 0;
                }
                ledgerHandle.asyncReadEntries(startEntryId, lastEntryId, (rc, lh, seq, ctx) -> {
                    if (rc == BookieException.Code.OK) {
                        List<LedgerEntryWrapper> ledgerEntries = new LinkedList<>();
                        while (seq.hasMoreElements()) {
                            org.apache.bookkeeper.client.LedgerEntry entry = seq.nextElement();
                            ledgerEntries.add(new LedgerEntryWrapperImpl(entry.getEntry(), new Position(entry.getLedgerId(), entry.getEntryId())));
                        }
                        // TODO: 2017/12/4 Move to next ledger if not able to read enough entries.
                        callback.readEntryComplete(ledgerEntries);
                    } else {
                        callback.readEntryFailed(new LedgerStorageException(BKException.create(rc)));
                    }
                }, null);
            }).exceptionally(throwable -> {
                callback.readEntryFailed(new LedgerStorageException(throwable));
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
                LedgerHandle ledgerHandle = this.bookKeeper.openLedger(ledgerId, this.bookKeeperConfig.getDigestType(), this.bookKeeperConfig.getPassword());
                this.bookKeeper.asyncOpenLedger(ledgerId, this.bookKeeperConfig.getDigestType(), this.bookKeeperConfig.getPassword(), (rc, lh, ctx) -> {
                    if (rc != BKException.Code.OK) {
                        // Remove from cache to let another thread reopen it
                        ledgerCache.remove(ledgerId, future);
                        future.completeExceptionally(new LedgerStorageException(BKException.getMessage(rc)));
                    } else {
                        logger.debug("[{}] Successfully opened ledger {} for reading", name, lh.getId());
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

    @Override public void close() throws InterruptedException, LedgerStorageException {
        if (state.get() == State.CLOSED) {
            logger.warn("LedgerManager is closed,so we just ignore this close quest.");
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
            throw new LedgerStorageException("LedgerManager " + name + " has already been closed");
        }
    }

    public LedgerHandle getCurrentLedgerHandle() {
        return currentLedgerHandle;
    }

    public LedgerManagerStorage getLedgerManagerStorage() {
        return ledgerManagerStorage;
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
