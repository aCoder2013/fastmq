package com.song.fastmq.storage.storage.impl;

import com.song.fastmq.storage.common.concurrent.SimpleThreadFactory;
import com.song.fastmq.storage.common.utils.Result;
import com.song.fastmq.storage.storage.LogReader;
import com.song.fastmq.storage.storage.LogReaderInfo;
import com.song.fastmq.storage.storage.LogRecord;
import com.song.fastmq.storage.storage.Offset;
import com.song.fastmq.storage.storage.OffsetStorage;
import com.song.fastmq.storage.storage.Version;
import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks;
import com.song.fastmq.storage.storage.concurrent.CommonPool;
import com.song.fastmq.storage.storage.support.LedgerStorageException;
import com.song.fastmq.storage.storage.support.ReadEntryCommand;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author song
 */
public class LogReaderImpl implements LogReader {

    private static final Logger logger = LoggerFactory.getLogger(LogReaderImpl.class);

    /**
     * Name of the consumer
     */
    private final String name;

    private final LogManagerImpl ledgerManager;

    private final OffsetStorage offsetStorage;

    private volatile Offset readOffset;

    private Version currentVersion;

    private final AsyncCuratorFramework asyncCuratorFramework;

    private final ScheduledExecutorService scheduledPersistPositionPool;
    private final LogReaderInfo logReaderInfo;

    public LogReaderImpl(String name, LogManagerImpl manager, OffsetStorage offsetStorage,
        AsyncCuratorFramework asyncCuratorFramework) {
        this.name = name;
        this.asyncCuratorFramework = asyncCuratorFramework;
        this.ledgerManager = manager;
        this.offsetStorage = offsetStorage;
        this.logReaderInfo = new LogReaderInfo(this.ledgerManager.getName(), this.name);
        this.scheduledPersistPositionPool = Executors.newScheduledThreadPool(1,
            new SimpleThreadFactory("Scheduled-persist-read-position-pool"));
    }

    public void init() throws Throwable {
        readOffset = this.offsetStorage.queryOffset(this.logReaderInfo);
        logger.info("[[]] current read position is {}.", readOffset.toString());
        this.scheduledPersistPositionPool
            .scheduleAtFixedRate(this::persistReadPosition, 10, 10, TimeUnit.SECONDS);
    }

    private void persistReadPosition() {
        try {
            this.offsetStorage
                .persistOffset(new LogReaderInfo(this.ledgerManager.getName(), this.name));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
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
                    logger.info("[{}] current position {}.", name, readOffset.toString());
                    callback.readEntryComplete(entries);
                }

                @Override
                public void readEntryFailed(Throwable throwable) {
                    if (throwable instanceof InvalidLedgerException) {
                        logger.info(
                            "Invalid ledgerId ,maybe this ledger was deleted,move to next one {}.",
                            readOffset.getLedgerId() + 1);
                        updateReadPosition(new Offset(readOffset.getLedgerId() + 1, 0));
                        CommonPool.executeBlocking(() -> asyncReadEntries(maxNumberToRead, callback));
                        return;
                    }
                    callback.readEntryFailed(throwable);
                }
            }));
    }

    @Override
    public void updateReadPosition(Offset offset) {
        this.readOffset = offset;
        this.offsetStorage.commitOffset(logReaderInfo, offset);
    }

    public Offset getReadOffset() {
        return readOffset;
    }

    @Override
    public void close() {
        this.scheduledPersistPositionPool.shutdown();
        persistReadPosition();
    }
}
