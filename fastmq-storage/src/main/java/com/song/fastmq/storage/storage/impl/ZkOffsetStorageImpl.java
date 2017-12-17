package com.song.fastmq.storage.storage.impl;

import com.song.fastmq.storage.common.utils.JsonUtils;
import com.song.fastmq.storage.common.utils.JsonUtils.JsonException;
import com.song.fastmq.storage.common.utils.Result;
import com.song.fastmq.storage.storage.LogInfoStorage;
import com.song.fastmq.storage.storage.LogReaderInfo;
import com.song.fastmq.storage.storage.Offset;
import com.song.fastmq.storage.storage.OffsetStorage;
import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks.ReadOffsetCallback;
import com.song.fastmq.storage.storage.metadata.Log;
import com.song.fastmq.storage.storage.metadata.LogSegment;
import com.song.fastmq.storage.storage.support.LedgerStorageException;
import com.song.fastmq.storage.storage.support.OffsetStorageException;
import com.song.fastmq.storage.storage.utils.ZkUtils;
import java.util.EnumSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.api.CreateOption;
import org.apache.curator.x.async.api.DeleteOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author song
 */
public class ZkOffsetStorageImpl implements OffsetStorage {

    private static final Logger logger = LoggerFactory.getLogger(ZkOffsetStorageImpl.class);

    private static final String ZK_OFFSET_STORAGE_PREFIX = "/fastmq/offset";

    private static final String ZK_OFFSET_STORAGE_PREFIX_PATH = ZK_OFFSET_STORAGE_PREFIX + "/";

    private final LogInfoStorage logInfoStorage;

    private final AsyncCuratorFramework asyncCuratorFramework;

    private final ExecutorService offsetThreadPool;

    private final ConcurrentMap<LogReaderInfo, Offset> offsetCache = new ConcurrentHashMap<>();

    public ZkOffsetStorageImpl(LogInfoStorage logInfoStorage,
        AsyncCuratorFramework asyncCuratorFramework) {
        this.logInfoStorage = logInfoStorage;
        this.asyncCuratorFramework = asyncCuratorFramework;
        offsetThreadPool = Executors.newSingleThreadExecutor(
            new BasicThreadFactory.Builder().uncaughtExceptionHandler((t, e) -> logger
                .error("Uncaught exception of thread :" + t.getClass().getName(), e))
                .build());
    }

    @Override
    public void commitOffset(LogReaderInfo logReaderInfo, Offset offset) {
        this.offsetCache.put(logReaderInfo, offset);
    }

    @Override
    public Offset queryOffset(LogReaderInfo logReaderInfo) throws OffsetStorageException {
        Result<Offset> result = new Result<>();
        asyncQueryOffset(logReaderInfo, new ReadOffsetCallback() {
            @Override
            public void onComplete(Offset offset) {
                result.setData(offset);
            }

            @Override
            public void onThrowable(Throwable throwable) {
                result.setThrowable(throwable);
            }
        });
        try {
            return result.getData();
        } catch (Throwable throwable) {
            throw new OffsetStorageException(throwable);
        }
    }

    @Override
    public void asyncQueryOffset(final LogReaderInfo logReaderInfo, ReadOffsetCallback callback) {
        if (this.offsetCache.containsKey(logReaderInfo)) {
            callback.onComplete(this.offsetCache.get(logReaderInfo));
            return;
        }
        AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        Offset offset = this.offsetCache.computeIfAbsent(logReaderInfo, reader -> {
            CompletableFuture<Offset> future = asyncEnsureOffsetExist(logReaderInfo, callback);
            try {
                return future.get();
            } catch (InterruptedException e) {
                throwableReference.set(e);
            } catch (ExecutionException e) {
                throwableReference.set(e.getCause());
            }
            return null;
        });
        if (offset == null) {
            callback.onThrowable(throwableReference.get());
            return;
        }
        callback.onComplete(offset);
    }

    @Override
    public void persistOffset(LogReaderInfo logReaderInfo) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        if (this.offsetCache.containsKey(logReaderInfo)) {
            Offset offset = this.offsetCache.get(logReaderInfo);
            String path = getReaderPath(logReaderInfo);
            try {
                this.asyncCuratorFramework.setData()
                    .forPath(path, JsonUtils.toJson(offset).getBytes())
                    .whenComplete((stat, throwable) -> {
                        if (throwable != null) {
                            logger.error("Persist offset failed", throwable);
                        } else {
                            logger.info(
                                "Successfully persist offset for consumer[{}] of topic [{}] : {}",
                                logReaderInfo.getLogReaderName(), logReaderInfo.getLogName(),
                                offset.toString());
                        }
                        latch.countDown();
                    });
            } catch (JsonException e) {
                logger.error("Convert to json failed", e);
            }
            latch.await();
        } else {
            logger.warn("Topic[{}]-consumer[{}] offset doesn't exist.", logReaderInfo.getLogName(),
                logReaderInfo.getLogReaderName());
        }
    }

    @Override
    public void removeOffset(LogReaderInfo logReaderInfo) {
        this.asyncCuratorFramework.delete().withOptions(EnumSet.of(DeleteOption.guaranteed))
            .forPath(getReaderPath(logReaderInfo)).whenComplete((aVoid, throwable) -> {
            if (throwable != null) {
                logger.error("Delete consumer offset failed", throwable);
            } else {
                logger
                    .info("Consumer[{}] offset has been safely deleted.", logReaderInfo.toString());
            }
        });
    }

    /**
     * Async get offset from zookeeper, if it doesn't exist then create a offset
     * with default setting.
     */
    private CompletableFuture<Offset> asyncEnsureOffsetExist(LogReaderInfo logReaderInfo,
        ReadOffsetCallback callback) {
        CompletableFuture<Offset> future = new CompletableFuture<>();
        String path = getReaderPath(logReaderInfo);
        this.asyncCuratorFramework.checkExists()
            .forPath(path)
            .whenComplete((stat, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(throwable);
                    return;
                }
                if (stat == null) {
                    offsetThreadPool.submit(() -> {
                        Log log;
                        try {
                            log = logInfoStorage.getLogInfo(logReaderInfo.getLogName());
                        } catch (InterruptedException | LedgerStorageException e) {
                            future.completeExceptionally(e);
                            return;
                        }
                        log.getSegments()
                            .sort((o1, o2) -> (int) (o1.getLedgerId() - o2.getLedgerId()));
                        LogSegment logSegment = log.getSegments().get(0);
                        logger.info(JsonUtils.toJsonQuietly(logSegment));
                        long ledgerId = logSegment.getLedgerId();
                        try {
                            byte[] bytes = JsonUtils.toJson(new Offset(ledgerId, 0)).getBytes();
                            asyncCuratorFramework.create()
                                .withOptions(EnumSet.of(CreateOption.createParentsIfNeeded))
                                .forPath(path, bytes)
                                .whenComplete((s, throwable1) -> {
                                    if (throwable1 != null) {
                                        future.completeExceptionally(throwable1);
                                    } else {
                                        try {
                                            future.complete(JsonUtils
                                                .fromJson(new String(bytes), Offset.class));
                                        } catch (JsonException e) {
                                            future.completeExceptionally(e);
                                        }
                                    }
                                });
                        } catch (JsonException e) {
                            future.completeExceptionally(e);
                        }
                    });
                } else {
                    this.offsetThreadPool
                        .submit(() -> asyncCuratorFramework.getData().forPath(path)
                            .whenComplete((bytes, throwable1) -> {
                                if (throwable1 != null) {
                                    callback.onThrowable(throwable1);
                                    return;
                                }
                                try {
                                    Offset offset = JsonUtils
                                        .fromJson(new String(bytes), Offset.class);
                                    future.complete(offset);
                                } catch (JsonException e) {
                                    future.completeExceptionally(e);
                                }
                            }));
                }
            });
        return future;
    }

    private String getReaderPath(LogReaderInfo logReaderInfo) {
        return ZK_OFFSET_STORAGE_PREFIX_PATH + logReaderInfo.getLogName() + ZkUtils.SEPERATOR
            + logReaderInfo
            .getLogReaderName();
    }
}
