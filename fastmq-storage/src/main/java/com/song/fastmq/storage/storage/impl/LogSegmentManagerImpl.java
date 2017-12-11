package com.song.fastmq.storage.storage.impl;

import com.song.fastmq.storage.common.concurrent.SimpleThreadFactory;
import com.song.fastmq.storage.common.utils.JsonUtils;
import com.song.fastmq.storage.common.utils.Result;
import com.song.fastmq.storage.storage.LogSegmentManager;
import com.song.fastmq.storage.storage.metadata.LogInfo;
import com.song.fastmq.storage.storage.LogRecord;
import com.song.fastmq.storage.storage.metadata.LogSegmentInfo;
import com.song.fastmq.storage.storage.support.LedgerStorageException;
import com.song.fastmq.storage.storage.Position;
import com.song.fastmq.storage.storage.Version;
import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks;
import com.song.fastmq.storage.storage.concurrent.CommonPool;
import com.song.fastmq.storage.storage.support.ReadEntryCommand;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.api.CreateOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author song
 */
public class LogSegmentManagerImpl implements LogSegmentManager {

    private static final Logger logger = LoggerFactory.getLogger(LogSegmentManagerImpl.class);

    private static final String LEDGER_CURSOR_PREFIX_NAME = "/fastmq/ledger-cursors";

    private static final String LEDGER_CURSOR_PREFIX = LEDGER_CURSOR_PREFIX_NAME + "/";

    /**
     * Name of the consumer
     */
    private final String name;

    private final String ledgerCursorFullPath;

    private final LogManagerImpl ledgerManager;

    private volatile Position readPosition;

    private Version currentVersion;

    private final AsyncCuratorFramework asyncCuratorFramework;

    private long getDataFromZKTimeoutMill = 3000;

    private ScheduledExecutorService scheduledPersistPositionPool = null;

    public LogSegmentManagerImpl(String name, LogManagerImpl manager,
        AsyncCuratorFramework asyncCuratorFramework) {
        this.name = name;
        this.asyncCuratorFramework = asyncCuratorFramework;
        this.ledgerManager = manager;
        this.ledgerCursorFullPath =
            LEDGER_CURSOR_PREFIX + this.ledgerManager.getName() + "/" + name;
        this.scheduledPersistPositionPool = Executors.newScheduledThreadPool(1,
            new SimpleThreadFactory("Scheduled-persist-read-position-pool"));
    }

    public void init() throws Throwable {
        class Result {

            byte[] data;
            Throwable throwable;
        }
        CountDownLatch latch = new CountDownLatch(1);
        Result result = new Result();
        asyncCuratorFramework.checkExists().forPath(this.ledgerCursorFullPath)
            .whenComplete((stat, throwable) -> {
                if (stat == null) {
                    CommonPool.executeBlocking(() -> {
                        LogInfo ledgerManager;
                        try {
                            ledgerManager = this.ledgerManager.getLogInfoStorage()
                                .getLogInfo(this.ledgerManager.getName());
                        } catch (InterruptedException | LedgerStorageException e) {
                            result.throwable = e;
                            latch.countDown();
                            return;
                        }
                        List<LogSegmentInfo> ledgers = ledgerManager.getLedgers();
                        ledgers.sort((o1, o2) -> (int) (o1.getLedgerId() - o2.getLedgerId()));
                        LogSegmentInfo logSegmentInfo = ledgers.get(0);
                        long ledgerId = logSegmentInfo.getLedgerId();
                        try {
                            byte[] bytes = JsonUtils.toJson(new Position(ledgerId, 0)).getBytes();
                            asyncCuratorFramework.create()
                                .withOptions(EnumSet.of(CreateOption.createParentsIfNeeded))
                                .forPath(this.ledgerCursorFullPath, bytes)
                                .whenComplete((s, throwable1) -> {
                                    if (throwable1 != null) {
                                        result.throwable = throwable1;
                                    } else {
                                        result.data = bytes;
                                    }
                                    latch.countDown();
                                });
                        } catch (JsonUtils.JsonException e) {
                            result.throwable = e;
                            latch.countDown();
                        }
                    });
                } else {
                    CommonPool.executeBlocking(
                        () -> asyncCuratorFramework.getData().forPath(this.ledgerCursorFullPath)
                            .whenComplete((bytes, t) -> {
                                if (throwable != null) {
                                    result.throwable = t;
                                } else {
                                    result.data = bytes;
                                }
                                latch.countDown();
                            }));
                }
            });

        if (!latch.await(getDataFromZKTimeoutMill, TimeUnit.MILLISECONDS)) {
            throw new LedgerStorageException(
                "Get cursor from zk timeout after" + getDataFromZKTimeoutMill + "mill seconds.");
        }
        if (result.throwable != null) {
            throw result.throwable;
        }
        // TODO: 2017/11/27 Store offset in BookKeeper instead of zookeeper
        readPosition = JsonUtils.fromJson(new String(result.data), Position.class);
        logger.info("[[]] current read position is {}.", readPosition.toString());
        this.scheduledPersistPositionPool
            .scheduleAtFixedRate(this::persistReadPosition, 10, 10, TimeUnit.SECONDS);
    }

    private void persistReadPosition() {
        try {
            asyncCuratorFramework.setData()
                .forPath(this.ledgerCursorFullPath, JsonUtils.toJson(readPosition).getBytes());
            logger.info("Persist read position of consumer[{}] with ledgerId {} and entryId {}.",
                this.ledgerManager.getName(), readPosition.getLedgerId(),
                readPosition.getEntryId());
        } catch (JsonUtils.JsonException e) {
            //Shouldn't happen, but you know...
            logger.error("Failed to persist read position_" + e.getMessage(), e);
        }
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public List<LogRecord> readEntries(int maxNumberToRead)
        throws InterruptedException, LedgerStorageException {
        Result<List<LogRecord>> result = new Result<>();
        asyncReadEntries(maxNumberToRead, new AsyncCallbacks.ReadEntryCallback() {
            @Override
            public void readEntryComplete(List<LogRecord> entries) {
                result.setData(entries);
            }

            @Override
            public void readEntryFailed(Throwable throwable) {
                result.setThrowable(throwable);
            }
        });
        try {
            return result.getData();
        } catch (Throwable throwable) {
            if (throwable instanceof InterruptedException) {
                throw (InterruptedException) throwable;
            }
            throw new LedgerStorageException(throwable);
        }
    }

    @Override
    public void asyncReadEntries(int maxNumberToRead, AsyncCallbacks.ReadEntryCallback callback) {
        this.ledgerManager.asyncReadEntries(
            new ReadEntryCommand(this, maxNumberToRead, new AsyncCallbacks.ReadEntryCallback() {
                @Override
                public void readEntryComplete(List<LogRecord> entries) {
                    logger.info("[{}] current position {}.", name, readPosition.toString());
                    callback.readEntryComplete(entries);
                }

                @Override
                public void readEntryFailed(Throwable throwable) {
                    if (throwable instanceof InvalidLedgerException) {
                        logger.info(
                            "Invalid ledgerId ,maybe this ledger was deleted,move to next one {}.",
                            readPosition.getLedgerId() + 1);
                        // TODO: 2017/12/7 read next valid ledgerId from zk
                        readPosition = new Position(readPosition.getLedgerId() + 1, 0);
                        callback.readEntryComplete(Collections.emptyList());
                        return;
                    }
                    callback.readEntryFailed(throwable);
                }
            }));
    }

    @Override
    public void updateReadPosition(Position position) {
        this.readPosition = position;
    }

    public Position getReadPosition() {
        return readPosition;
    }

    @Override
    public void close() {
        persistReadPosition();
        this.scheduledPersistPositionPool.shutdown();
    }
}
