package com.song.fastmq.broker.storage.impl;

import com.song.fastmq.broker.storage.AsyncCallback;
import com.song.fastmq.broker.storage.CommonPool;
import com.song.fastmq.broker.storage.LedgerManager;
import com.song.fastmq.broker.storage.LedgerMetadata;
import com.song.fastmq.broker.storage.LedgerStorageException;
import com.song.fastmq.broker.storage.LedgerStream;
import com.song.fastmq.broker.storage.LedgerStreamStorage;
import com.song.fastmq.broker.storage.Version;
import com.song.fastmq.broker.storage.config.BookKeeperConfig;
import java.util.LinkedList;
import java.util.NavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.bookie.BookieException;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.commons.collections.CollectionUtils;

/**
 * Created by song on 2017/11/5.
 */
public class DefaultLedgerManager implements LedgerManager {

    private final String name;

    private volatile int ledgerVersion;

    private volatile LedgerHandle currentLedgerHandle;

    private final BookKeeperConfig bookKeeperConfig;

    private final BookKeeper bookKeeper;

    private final LedgerStreamStorage ledgerStreamStorage;

    private final NavigableMap<Long, LedgerMetadata> ledgers = new ConcurrentSkipListMap<>();

    private final AtomicReference<State> state = new AtomicReference<>();

    public DefaultLedgerManager(String name, BookKeeperConfig config,
        BookKeeper bookKeeper,
        LedgerStreamStorage storage) {
        this.name = name;
        bookKeeperConfig = config;
        this.bookKeeper = bookKeeper;
        this.ledgerStreamStorage = storage;
        this.state.set(State.NONE);
    }

    public void init(AsyncCallback<Void, LedgerStorageException> asyncCallback) {
        CommonPool.executeBlocking(() -> ledgerStreamStorage.asyncGetLedgerStream(name, new AsyncCallback<LedgerStream, LedgerStorageException>() {
            @Override public void onCompleted(LedgerStream result, Version version) {
                ledgerVersion = version.getVersion();
                if (CollectionUtils.isNotEmpty(result.getLedgers())) {
                    result.getLedgers().forEach(metadata -> ledgers.put(metadata.getLedgerId(), metadata));
                }
                bookKeeper.asyncCreateLedger(bookKeeperConfig.getEnsSize(), bookKeeperConfig.getWriteQuorumSize(),
                    bookKeeperConfig.getDigestType(), bookKeeperConfig.getPassword(), (rc, lh, ctx) -> {
                        if (rc == BookieException.Code.OK) {
                            long ledgerId = lh.getId();
                            LedgerMetadata ledgerMetadata = new LedgerMetadata();
                            ledgerMetadata.setLedgerId(ledgerId);
                            ledgerMetadata.setTimestamp(System.currentTimeMillis());
                            if (result.getLedgers() == null) {
                                result.setLedgers(new LinkedList<>());
                            }
                            result.getLedgers().add(ledgerMetadata);
                            ledgerStreamStorage.asyncUpdateLedgerStream(name, result, version, new AsyncCallback<Void, LedgerStorageException>() {
                                @Override public void onCompleted(Void result, Version version) {
                                    ledgerVersion = version.getVersion();
                                    ledgers.put(ledgerId, ledgerMetadata);
                                    currentLedgerHandle = lh;
                                    state.set(State.LEDGER_OPENED);
                                    asyncCallback.onCompleted(null, version);
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
    }

    @Override public String getName() {
        return name;
    }

    @Override public void addEntry(byte[] data) throws InterruptedException, LedgerStorageException {
        try {
            this.currentLedgerHandle.addEntry(data);
        } catch (BKException e) {
            throw new LedgerStorageException(e);
        }
    }

    @Override public void asyncAddEntry(byte[] data, AsyncCallback<Void, LedgerStorageException> asyncCallback) {
        this.currentLedgerHandle.asyncAddEntry(data, (rc, lh, entryId, ctx) -> {
            if (rc == BookieException.Code.OK) {
                asyncCallback.onCompleted(null, new ZkVersion(0));
            } else {
                asyncCallback.onThrowable(new LedgerStorageException(BookieException.create(rc)));
            }
        }, null);
    }

    public LedgerHandle getCurrentLedgerHandle() {
        return currentLedgerHandle;
    }

    enum State {
        NONE,
        LEDGER_OPENED,
        LEDGER_CLOSING,
        LEDGER_CLOSED,
        LEDGER_CREATING,
        CLOSED,
        FENCED,
    }
}
