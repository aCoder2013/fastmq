package com.song.fastmq.broker.storage.impl;

import com.song.fastmq.broker.storage.LedgerCursor;
import com.song.fastmq.broker.storage.LedgerEntryWrapper;
import com.song.fastmq.broker.storage.LedgerManager;
import com.song.fastmq.broker.storage.LedgerStorageException;
import com.song.fastmq.broker.storage.Position;
import com.song.fastmq.broker.storage.Version;
import com.song.fastmq.broker.storage.concurrent.AsyncCallbacks;
import com.song.fastmq.common.concurrent.SimpleThreadFactory;
import com.song.fastmq.common.utils.JsonUtils;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author song
 */
public class LedgerCursorImpl implements LedgerCursor {

    private static final Logger logger = LoggerFactory.getLogger(LedgerCursorImpl.class);

    private static final String LEDGER_CURSOR_PREFIX_NAME = "/fastmq/ledger-cursors";

    private static final String LEDGER_CURSOR_PREFIX = LEDGER_CURSOR_PREFIX_NAME + "/";

    /**
     * Name of the consumer
     */
    private final String name;

    private final LedgerManager ledgerManager;

    private Position readPosition;

    private Version currentVersion;

    private final ZooKeeper zookeeper;

    private long getDataFromZKTimeoutMill = 3000;

    private ScheduledExecutorService scheduledPersistPositionPool = null;

    public LedgerCursorImpl(String name, LedgerManager manager, ZooKeeper zookeeper) {
        this.name = name;
        this.ledgerManager = manager;
        this.zookeeper = zookeeper;
        this.scheduledPersistPositionPool = Executors.newScheduledThreadPool(1, new SimpleThreadFactory("Persist-read-position-pool"));
    }

    public void init() throws Exception {
        class Result {
            byte[] data;
            Exception exception;
        }
        CountDownLatch latch = new CountDownLatch(1);
        Result result = new Result();
        this.zookeeper.getData(LEDGER_CURSOR_PREFIX + name, false, (rc, path, ctx, data, stat) -> {
            if (rc == KeeperException.Code.OK.intValue()) {
                result.data = data;
                currentVersion = new ZkVersion(stat.getVersion());
                latch.countDown();
            } else if (rc == KeeperException.Code.NONODE.intValue()) {
                try {
                    byte[] bytes = JsonUtils.toJson(new Position(-1, -1)).getBytes();
                    ZkUtils.asyncCreateFullPathOptimistic(zookeeper, LEDGER_CURSOR_PREFIX + name, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                        (rc1, path1, ctx1, name1) -> {
                            if (rc1 == KeeperException.Code.OK.intValue()) {
                                result.data = bytes;
                                currentVersion = new ZkVersion(0);
                            } else {
                                result.exception = KeeperException.create(KeeperException.Code.get(rc));
                            }
                            latch.countDown();
                        }, null);
                } catch (JsonUtils.JsonException e) {
                    result.exception = e;
                    latch.countDown();
                }
            } else {
                result.exception = KeeperException.create(KeeperException.Code.get(rc));
                latch.countDown();
            }
        }, null);

        if (!latch.await(getDataFromZKTimeoutMill, TimeUnit.MILLISECONDS)) {
            throw new LedgerStorageException("Get cursor from zk timeout after" + getDataFromZKTimeoutMill + "mill seconds.");
        }
        if (result.exception != null) {
            throw result.exception;
        }
        // TODO: 2017/11/27 Store offset in bookeeper instead of zookeeper
        readPosition = JsonUtils.fromJson(new String(result.data), Position.class);
        this.scheduledPersistPositionPool.scheduleAtFixedRate(this::persistReadPosition, 10, 5, TimeUnit.SECONDS);
    }

    private void persistReadPosition() {
        try {
            Stat stat = zookeeper.setData(LEDGER_CURSOR_PREFIX + name, JsonUtils.toJson(readPosition).getBytes(), currentVersion.getVersion());
            this.currentVersion = new ZkVersion(stat.getVersion());
        } catch (KeeperException | InterruptedException | JsonUtils.JsonException e) {
            logger.error("Persist read position failed_" + e.getMessage(), e);
        }
    }

    @Override public String name() {
        return name;
    }

    @Override
    public List<LedgerEntryWrapper> readEntries(int numberToRead) throws InterruptedException, LedgerStorageException {
        List<LedgerEntryWrapper> wrappers = this.ledgerManager.readEntries(numberToRead, readPosition);
        readPosition = new Position(readPosition.getLedgerId(), readPosition.getEntryId() + numberToRead);
        return wrappers;
    }

    @Override public void asyncReadEntries(int numberToRead, AsyncCallbacks.ReadEntryCallback callback) {
        this.ledgerManager.asyncReadEntries(numberToRead, readPosition, callback);
    }

    @Override public void close() {
        persistReadPosition();
        this.scheduledPersistPositionPool.shutdown();
    }

    @Override public void asyncClose(AsyncCallbacks.CloseLedgerCursorCallback callback) {
        persistReadPosition();
        this.scheduledPersistPositionPool.shutdown();
    }
}
