package com.song.fastmq.broker.storage.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.song.fastmq.broker.storage.LedgerInfoManager;
import com.song.fastmq.broker.storage.LedgerManagerStorage;
import com.song.fastmq.broker.storage.LedgerStorageException;
import com.song.fastmq.broker.storage.Version;
import com.song.fastmq.broker.storage.concurrent.AsyncCallback;
import com.song.fastmq.broker.storage.concurrent.CommonPool;
import com.song.fastmq.common.utils.JsonUtils;
import java.util.EnumSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.api.CreateOption;
import org.apache.curator.x.async.api.DeleteOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by song on 2017/11/5.
 */
public class LedgerManagerStorageImpl implements LedgerManagerStorage {

    private static final Logger logger = LoggerFactory.getLogger(LedgerManagerStorageImpl.class);

    private static final String LEDGER_NAME_PREFIX_NAME = "/fastmq/bk-ledgers";

    private static final String LEDGER_NAME_PREFIX = LEDGER_NAME_PREFIX_NAME + "/";

    private final AsyncCuratorFramework asyncCuratorFramework;

    public LedgerManagerStorageImpl(AsyncCuratorFramework asyncCuratorFramework) throws Exception {
        this.asyncCuratorFramework = asyncCuratorFramework;
    }

    @Override
    public LedgerInfoManager getLedgerManager(String ledgerName) throws InterruptedException, LedgerStorageException {
        final LedgerResult ledgerResult = new LedgerResult();
        CountDownLatch latch = new CountDownLatch(1);
        asyncGetLedgerManager(ledgerName, new AsyncCallback<LedgerInfoManager, LedgerStorageException>() {

            @Override public void onCompleted(LedgerInfoManager result, Version version) {
                ledgerResult.ledgerInfoManager = result;
                latch.countDown();
            }

            @Override public void onThrowable(LedgerStorageException e) {
                ledgerResult.exception = e;
                latch.countDown();
            }
        });
        latch.await();
        if (ledgerResult.exception != null) {
            throw ledgerResult.exception;
        }
        return ledgerResult.ledgerInfoManager;
    }

    @Override public void asyncGetLedgerManager(String name,
        AsyncCallback<LedgerInfoManager, LedgerStorageException> asyncCallback) {
        String ledgerManagerPath = LEDGER_NAME_PREFIX + name;
        this.asyncCuratorFramework.checkExists().forPath(ledgerManagerPath).whenComplete((stat, throwable) -> {
            if (throwable != null) {
                asyncCallback.onThrowable(new LedgerStorageException(throwable));
                return;
            }
            if (stat == null) {
                CommonPool.executeBlocking(() -> {
                    logger.info("Create ledger [{}]", name);
                    LedgerInfoManager ledgerInfoManager = new LedgerInfoManager();
                    ledgerInfoManager.setName(name);
                    byte[] bytes;
                    try {
                        bytes = JsonUtils.get().writeValueAsBytes(ledgerInfoManager);
                    } catch (JsonProcessingException e) {
                        asyncCallback.onThrowable(new LedgerStorageException(e));
                        return;
                    }
                    this.asyncCuratorFramework.create().withOptions(EnumSet.of(CreateOption.createParentsIfNeeded)).forPath(ledgerManagerPath, bytes).whenComplete((s, throwable1) -> {
                        if (throwable1 != null) {
                            asyncCallback.onThrowable(new LedgerStorageException(throwable1));
                        } else {
                            asyncCallback.onCompleted(ledgerInfoManager, new ZkVersion(0));
                        }
                    });
                });
            } else {
                this.asyncCuratorFramework.getData().forPath(ledgerManagerPath).whenComplete((bytes, throwable1) -> {
                    if (throwable1 != null) {
                        asyncCallback.onThrowable(new LedgerStorageException(throwable1));
                    } else {
                        LedgerInfoManager ledgerInfoManager = null;
                        try {
                            ledgerInfoManager = JsonUtils.fromJson(new String(bytes), LedgerInfoManager.class);
                        } catch (JsonUtils.JsonException e) {
                            asyncCallback.onThrowable(new LedgerStorageException(e));
                            return;
                        }
                        asyncCallback.onCompleted(ledgerInfoManager, new ZkVersion(stat.getVersion()));
                    }
                });
            }
        });
    }

    @Override public void asyncUpdateLedgerManager(String name, LedgerInfoManager ledgerInfoManager, Version version,
        AsyncCallback<Void, LedgerStorageException> asyncCallback) {
        CommonPool.executeBlocking(() -> {
            byte[] bytes;
            try {
                bytes = JsonUtils.get().writeValueAsBytes(ledgerInfoManager);
            } catch (JsonProcessingException e) {
                asyncCallback.onThrowable(new LedgerStorageException(e));
                return;
            }
            this.asyncCuratorFramework.setData().forPath(LEDGER_NAME_PREFIX + name, bytes).whenComplete((stat, throwable) -> {
                if (throwable != null) {
                    asyncCallback.onThrowable(new LedgerStorageException(throwable));
                } else {
                    asyncCallback.onCompleted(null, new ZkVersion(stat.getVersion()));
                }
            });
        });
    }

    @Override public void removeLedger(String name) throws InterruptedException, LedgerStorageException {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        asyncRemoveLedger(name, new AsyncCallback<Void, LedgerStorageException>() {
            @Override public void onCompleted(Void result, Version version) {
                completableFuture.complete(result);
            }

            @Override public void onThrowable(LedgerStorageException throwable) {
                completableFuture.completeExceptionally(throwable);
            }
        });
        try {
            completableFuture.get();
        } catch (ExecutionException e) {
            throw new LedgerStorageException(e.getCause());
        }
    }

    @Override public void asyncRemoveLedger(String name, AsyncCallback<Void, LedgerStorageException> asyncCallback) {
        logger.info("Remove ledger [{}].", name);
        this.asyncCuratorFramework.delete().withOptions(EnumSet.of(DeleteOption.guaranteed)).forPath(LEDGER_NAME_PREFIX + name).whenComplete((aVoid, throwable) -> {
            if (throwable != null) {
                asyncCallback.onThrowable(new LedgerStorageException(throwable));
            } else {
                asyncCallback.onCompleted(null, new ZkVersion(0));
            }
        });
    }

    class LedgerResult {
        LedgerInfoManager ledgerInfoManager;

        LedgerStorageException exception;
    }
}
