package com.song.fastmq.storage.storage.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.song.fastmq.storage.common.utils.JsonUtils;
import com.song.fastmq.storage.storage.metadata.LogInfo;
import com.song.fastmq.storage.storage.LogInfoStorage;
import com.song.fastmq.storage.storage.support.LedgerStorageException;
import com.song.fastmq.storage.storage.Version;
import com.song.fastmq.storage.storage.concurrent.AsyncCallback;
import com.song.fastmq.storage.storage.concurrent.CommonPool;
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
public class LogInfoStorageImpl implements LogInfoStorage {

    private static final Logger logger = LoggerFactory.getLogger(LogInfoStorageImpl.class);

    private static final String LEDGER_NAME_PREFIX_NAME = "/fastmq/bk-ledgers";

    private static final String LEDGER_NAME_PREFIX = LEDGER_NAME_PREFIX_NAME + "/";

    private final AsyncCuratorFramework asyncCuratorFramework;

    public LogInfoStorageImpl(AsyncCuratorFramework asyncCuratorFramework) throws Exception {
        this.asyncCuratorFramework = asyncCuratorFramework;
    }

    @Override
    public LogInfo getLogInfo(String ledgerName) throws InterruptedException, LedgerStorageException {
        final LedgerResult ledgerResult = new LedgerResult();
        CountDownLatch latch = new CountDownLatch(1);
        asyncGetLogInfo(ledgerName, new AsyncCallback<LogInfo, LedgerStorageException>() {

            @Override public void onCompleted(LogInfo data, Version version) {
                ledgerResult.logInfo = data;
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
        return ledgerResult.logInfo;
    }

    @Override public void asyncGetLogInfo(String name,
        AsyncCallback<LogInfo, LedgerStorageException> asyncCallback) {
        String ledgerManagerPath = LEDGER_NAME_PREFIX + name;
        this.asyncCuratorFramework.checkExists().forPath(ledgerManagerPath).whenComplete((stat, throwable) -> {
            if (throwable != null) {
                asyncCallback.onThrowable(new LedgerStorageException(throwable));
                return;
            }
            if (stat == null) {
                CommonPool.executeBlocking(() -> {
                    logger.info("Create ledger [{}]", name);
                    LogInfo logInfo = new LogInfo();
                    logInfo.setName(name);
                    byte[] bytes;
                    try {
                        bytes = JsonUtils.get().writeValueAsBytes(logInfo);
                    } catch (JsonProcessingException e) {
                        asyncCallback.onThrowable(new LedgerStorageException(e));
                        return;
                    }
                    this.asyncCuratorFramework.create().withOptions(EnumSet.of(CreateOption.createParentsIfNeeded)).forPath(ledgerManagerPath, bytes).whenComplete((s, throwable1) -> {
                        if (throwable1 != null) {
                            asyncCallback.onThrowable(new LedgerStorageException(throwable1));
                        } else {
                            asyncCallback.onCompleted(logInfo, new ZkVersion(0));
                        }
                    });
                });
            } else {
                this.asyncCuratorFramework.getData().forPath(ledgerManagerPath).whenComplete((bytes, throwable1) -> {
                    if (throwable1 != null) {
                        asyncCallback.onThrowable(new LedgerStorageException(throwable1));
                    } else {
                        LogInfo logInfo = null;
                        try {
                            logInfo = JsonUtils.fromJson(new String(bytes), LogInfo.class);
                        } catch (JsonUtils.JsonException e) {
                            asyncCallback.onThrowable(new LedgerStorageException(e));
                            return;
                        }
                        asyncCallback.onCompleted(logInfo, new ZkVersion(stat.getVersion()));
                    }
                });
            }
        });
    }

    @Override public void asyncUpdateLogInfo(String name, LogInfo logInfo, Version version,
        AsyncCallback<Void, LedgerStorageException> asyncCallback) {
        CommonPool.executeBlocking(() -> {
            byte[] bytes;
            try {
                bytes = JsonUtils.get().writeValueAsBytes(logInfo);
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

    @Override public void removeLogInfo(String name) throws InterruptedException, LedgerStorageException {
        CompletableFuture<Void> completableFuture = new CompletableFuture<>();
        asyncRemoveLogInfo(name, new AsyncCallback<Void, LedgerStorageException>() {
            @Override public void onCompleted(Void data, Version version) {
                completableFuture.complete(data);
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

    @Override public void asyncRemoveLogInfo(String name, AsyncCallback<Void, LedgerStorageException> asyncCallback) {
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
        LogInfo logInfo;

        LedgerStorageException exception;
    }
}
