package com.song.fastmq.storage.storage.impl;

import com.song.fastmq.storage.common.utils.JsonUtils;
import com.song.fastmq.storage.common.utils.JsonUtils.JsonException;
import com.song.fastmq.storage.common.utils.Result;
import com.song.fastmq.storage.storage.ConsumerInfo;
import com.song.fastmq.storage.storage.MetadataStorage;
import com.song.fastmq.storage.storage.Offset;
import com.song.fastmq.storage.storage.OffsetStorage;
import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks.ReadOffsetCallback;
import com.song.fastmq.storage.storage.metadata.LogSegment;
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

    private final MetadataStorage metadataStorage;

    private final AsyncCuratorFramework asyncCuratorFramework;

    private final ExecutorService offsetThreadPool;

    private final ConcurrentMap<ConsumerInfo, Offset> offsetCache = new ConcurrentHashMap<>();

    public ZkOffsetStorageImpl(MetadataStorage metadataStorage,
        AsyncCuratorFramework asyncCuratorFramework) {
        this.metadataStorage = metadataStorage;
        this.asyncCuratorFramework = asyncCuratorFramework;
        offsetThreadPool = Executors.newSingleThreadExecutor(
            new BasicThreadFactory.Builder().uncaughtExceptionHandler((t, e) -> logger
                .error("Uncaught exception of thread :" + t.getClass().getName(), e))
                .build());
    }

    @Override
    public void commitOffset(ConsumerInfo consumerInfo, Offset offset) {
        this.offsetCache.put(consumerInfo, offset);
    }

    @Override
    public Offset queryOffset(ConsumerInfo consumerInfo) throws OffsetStorageException {
        Result<Offset> result = new Result<>();
        asyncQueryOffset(consumerInfo, new ReadOffsetCallback() {
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
            // TODO: 2018/2/20 Result is not a good design
            return result.getData();
        } catch (Throwable throwable) {
            throw new OffsetStorageException(throwable);
        }
    }

    @Override
    public void asyncQueryOffset(final ConsumerInfo consumerInfo, ReadOffsetCallback callback) {
        if (this.offsetCache.containsKey(consumerInfo)) {
            callback.onComplete(this.offsetCache.get(consumerInfo));
            return;
        }
        AtomicReference<Throwable> throwableReference = new AtomicReference<>();
        Offset offset = this.offsetCache.computeIfAbsent(consumerInfo, reader -> {
            CompletableFuture<Offset> future = asyncEnsureOffsetExist(consumerInfo, callback);
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
    public void persistOffset(ConsumerInfo consumerInfo) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        if (this.offsetCache.containsKey(consumerInfo)) {
            Offset offset = this.offsetCache.get(consumerInfo);
            String path = getReaderPath(consumerInfo);
            try {
                this.asyncCuratorFramework.setData()
                    .forPath(path, JsonUtils.toJson(offset).getBytes())
                    .whenComplete((stat, throwable) -> {
                        if (throwable != null) {
                            logger.error("Persist offset failed", throwable);
                        } else {
                            logger.info(
                                "Successfully persist offset for consumer[{}] of topic [{}] : {}",
                                consumerInfo.getConsumerName(), consumerInfo.getTopic(),
                                offset.toString());
                        }
                        latch.countDown();
                    });
            } catch (JsonException e) {
                logger.error("Convert to json failed", e);
            }
            latch.await();
        } else {
            logger.warn("Topic[{}]-consumer[{}] offset doesn't exist.", consumerInfo.getTopic(),
                consumerInfo.getConsumerName());
        }
    }

    @Override
    public void removeOffset(ConsumerInfo consumerInfo) {
        this.asyncCuratorFramework.delete().withOptions(EnumSet.of(DeleteOption.guaranteed))
            .forPath(getReaderPath(consumerInfo)).whenComplete((aVoid, throwable) -> {
            if (throwable != null) {
                logger.error("Delete consumer offset failed", throwable);
            } else {
                logger
                    .info("Consumer[{}] offset has been safely deleted.", consumerInfo.toString());
            }
        });
    }

    /**
     * Async get offset from zookeeper, if it doesn't exist then create a offset
     * with default setting.
     */
    private CompletableFuture<Offset> asyncEnsureOffsetExist(ConsumerInfo consumerInfo,
        ReadOffsetCallback callback) {
        CompletableFuture<Offset> future = new CompletableFuture<>();
        String path = getReaderPath(consumerInfo);
        this.asyncCuratorFramework.checkExists()
            .forPath(path)
            .whenComplete((stat, throwable) -> {
                if (throwable != null) {
                    future.completeExceptionally(throwable);
                    return;
                }
                if (stat == null) {
                    offsetThreadPool.submit(() -> metadataStorage.getLogInfo(consumerInfo.getTopic())
                        .subscribe(log -> {
                            long ledgerId = 0;
                            if (log.getSegments().size() > 0) {
                                log.getSegments().sort((o1, o2) -> (int) (o1.getLedgerId() - o2.getLedgerId()));
                                LogSegment logSegment = log.getSegments().get(0);
                                ledgerId = logSegment.getLedgerId();
                            }
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
                        }));
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

    private String getReaderPath(ConsumerInfo consumerInfo) {
        return ZK_OFFSET_STORAGE_PREFIX_PATH + consumerInfo.getTopic() + ZkUtils.INSTANCE.getSEPARATOR()
            + consumerInfo
            .getConsumerName();
    }
}
