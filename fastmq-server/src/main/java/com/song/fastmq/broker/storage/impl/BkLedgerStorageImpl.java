package com.song.fastmq.broker.storage.impl;

import com.song.fastmq.broker.storage.BkLedgerStorage;
import com.song.fastmq.broker.storage.LedgerManager;
import com.song.fastmq.broker.storage.LedgerManagerStorage;
import com.song.fastmq.broker.storage.LedgerStorageException;
import com.song.fastmq.broker.storage.Version;
import com.song.fastmq.broker.storage.concurrent.AsyncCallback;
import com.song.fastmq.broker.storage.concurrent.CommonPool;
import com.song.fastmq.broker.storage.config.BookKeeperConfig;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Default implementation of {@link BkLedgerStorage}
 * Created by song on 2017/11/4.
 */
public class BkLedgerStorageImpl implements BkLedgerStorage {

    private static final Logger logger = LoggerFactory.getLogger(BkLedgerStorageImpl.class);

    private volatile Version currentVersion;

    private final BookKeeperConfig bookKeeperConfig;

    private final ZooKeeper zooKeeper;

    private final BookKeeper bookKeeper;

    private final LedgerManagerStorage ledgerManagerStorage;

    private final ConcurrentMap<String, CompletableFuture<LedgerManager>> ledgers = new ConcurrentHashMap<>();

    public BkLedgerStorageImpl(ClientConfiguration clientConfiguration, BookKeeperConfig config)
        throws Exception {
        bookKeeperConfig = config;
        checkNotNull(clientConfiguration);
        String servers = clientConfiguration.getZkServers();
        checkNotNull(servers);
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        zooKeeper = new ZooKeeper(servers, clientConfiguration.getZkTimeout(), event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                logger.info("Connected to zookeeper ,connectString = {}", servers);
                countDownLatch.countDown();
            } else {
                logger.error("Failed to connect zookeeper,connectString = {}", servers);
            }
        });

        if (!countDownLatch.await(clientConfiguration.getZkTimeout(), TimeUnit.MILLISECONDS)
            || zooKeeper.getState() != ZooKeeper.States.CONNECTED) {
            throw new LedgerStorageException(
                "Error connecting to zookeeper server ,connectString = " + servers + ".");
        }

        this.bookKeeper = new BookKeeper(clientConfiguration, zooKeeper);
        ledgerManagerStorage = new LedgerManagerStorageImpl(zooKeeper);
    }

    @Override public LedgerManager open(String name)
        throws LedgerStorageException, InterruptedException {
        Result result = new Result();
        CountDownLatch latch = new CountDownLatch(1);
        asyncOpen(name, new AsyncCallback<LedgerManager, LedgerStorageException>() {
            @Override public void onCompleted(LedgerManager ledgerManager, Version version) {
                currentVersion = version;
                result.ledgerManager = ledgerManager;
                latch.countDown();
            }

            @Override public void onThrowable(LedgerStorageException throwable) {
                result.exception = throwable;
                latch.countDown();
            }
        });
        latch.await();
        if (result.exception != null) {
            throw result.exception;
        }
        return result.ledgerManager;
    }

    @Override public void asyncOpen(String name,
        AsyncCallback<LedgerManager, LedgerStorageException> asyncCallback) {
        CommonPool.executeBlocking(() -> {
            ledgers.computeIfAbsent(name, (mlName) -> {
                CompletableFuture<LedgerManager> future = new CompletableFuture<>();
                LedgerManagerImpl ledgerManager = new LedgerManagerImpl(mlName, bookKeeperConfig,
                    bookKeeper, zooKeeper, ledgerManagerStorage);
                ledgerManager.init(new AsyncCallback<Void, LedgerStorageException>() {
                    @Override public void onCompleted(Void result, Version version) {
                        currentVersion = version;
                        future.complete(ledgerManager);
                    }

                    @Override public void onThrowable(LedgerStorageException throwable) {
                        ledgers.remove(name);
                        future.completeExceptionally(throwable);
                    }
                });
                return future;
            }).thenAccept(manager -> asyncCallback.onCompleted(manager, null)).exceptionally(throwable -> {
                asyncCallback.onThrowable(new LedgerStorageException(throwable));
                return null;
            });
            CompletableFuture<LedgerManager> completableFuture = ledgers.get(name);
            if (completableFuture != null && completableFuture.isDone()) {
                try {
                    LedgerManager ledgerManager = completableFuture.get();
                    asyncCallback.onCompleted(ledgerManager, currentVersion);
                } catch (InterruptedException e) {
                    asyncCallback.onThrowable(new LedgerStorageException(e));
                } catch (ExecutionException e) {
                    asyncCallback.onThrowable(new LedgerStorageException(e.getCause()));
                }
            }
        });
    }

    class Result {
        LedgerManager ledgerManager;

        LedgerStorageException exception;
    }
}
