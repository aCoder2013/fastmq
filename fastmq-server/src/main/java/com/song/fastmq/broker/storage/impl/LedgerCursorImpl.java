package com.song.fastmq.broker.storage.impl;

import com.song.fastmq.broker.storage.LedgerCursor;
import com.song.fastmq.broker.storage.LedgerEntryWrapper;
import com.song.fastmq.broker.storage.LedgerInfo;
import com.song.fastmq.broker.storage.LedgerInfoManager;
import com.song.fastmq.broker.storage.LedgerStorageException;
import com.song.fastmq.broker.storage.Position;
import com.song.fastmq.broker.storage.Version;
import com.song.fastmq.broker.storage.concurrent.AsyncCallbacks;
import com.song.fastmq.broker.storage.concurrent.CommonPool;
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

    private final LedgerManagerImpl ledgerManager;

    // TODO: 2017/11/29 若首先启动，则需要读取最早的LedgerInfo
    private Position readPosition;

    private Version currentVersion;

    private final ZooKeeper zookeeper;

    // TODO: 2017/11/29 test模式超时时间
    private long getDataFromZKTimeoutMill = 3000000;

    private ScheduledExecutorService scheduledPersistPositionPool = null;

    public LedgerCursorImpl(String name, LedgerManagerImpl manager, ZooKeeper zookeeper) {
        this.name = name;
        this.ledgerManager = manager;
        this.zookeeper = zookeeper;
        this.scheduledPersistPositionPool = Executors.newScheduledThreadPool(1, new SimpleThreadFactory("Persist-read-position-pool"));
//        try {
//            ZkUtils.createFullPathOptimistic(zookeeper,LEDGER_CURSOR_PREFIX +this.ledgerManager.getName(),new byte[0],ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
//        } catch (KeeperException e) {
//            e.printStackTrace();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
    }

    public void init() throws Exception {
        class Result {
            byte[] data;
            Exception exception;
        }
        CountDownLatch latch = new CountDownLatch(1);
        Result result = new Result();
        this.zookeeper.getData(LEDGER_CURSOR_PREFIX + this.ledgerManager.getName() + "/" + name, false, (rc, path, ctx, data, stat) -> CommonPool.executeBlocking(() -> {
            if (rc == KeeperException.Code.OK.intValue()) {
                result.data = data;
                currentVersion = new ZkVersion(stat.getVersion());
                latch.countDown();
            } else if (rc == KeeperException.Code.NONODE.intValue()) {
                try {
                    // TODO: 2017/11/29 读取不到
                    LedgerInfoManager ledgerManager = this.ledgerManager.getLedgerManagerStorage().getLedgerManager(this.ledgerManager.getName());
                    List<LedgerInfo> ledgers = ledgerManager.getLedgers();
                    ledgers.sort((o1, o2) -> (int) (o1.getLedgerId() - o2.getLedgerId()));
                    LedgerInfo ledgerInfo = ledgers.get(0);
                    long ledgerId = ledgerInfo.getLedgerId();
                    byte[] bytes = JsonUtils.toJson(new Position(ledgerId, -1)).getBytes();
                    ZkUtils.asyncCreateFullPathOptimistic(zookeeper, LEDGER_CURSOR_PREFIX + this.ledgerManager.getName() + "/" + name, bytes, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT,
                        (rc1, path1, ctx1, name1) -> {
                            if (rc1 == KeeperException.Code.OK.intValue()) {
                                result.data = bytes;
                                currentVersion = new ZkVersion(0);
                            } else {
                                result.exception = KeeperException.create(KeeperException.Code.get(rc));
                            }
                            latch.countDown();
                        }, null);
                } catch (Exception e) {
                    result.exception = e;
                    latch.countDown();
                }
            } else {
                result.exception = KeeperException.create(KeeperException.Code.get(rc));
                latch.countDown();
            }
        }), null);

        if (!latch.await(getDataFromZKTimeoutMill, TimeUnit.MILLISECONDS)) {
            throw new LedgerStorageException("Get cursor from zk timeout after" + getDataFromZKTimeoutMill + "mill seconds.");
        }
        if (result.exception != null) {
            throw result.exception;
        }
        // TODO: 2017/11/27 Store offset in BookKeeper instead of zookeeper
        readPosition = JsonUtils.fromJson(new String(result.data), Position.class);
        this.scheduledPersistPositionPool.scheduleAtFixedRate(() -> {
            logger.info("Start to persist read position of consumer[{}].", name);
            persistReadPosition();
            logger.info("Finish to persist read position of consumer[{}].", name);
        }, 0, 3, TimeUnit.SECONDS);
    }

    private void persistReadPosition() {
        try {
            logger.info("Current read position:{}.", JsonUtils.toJsonQuietly(readPosition));
            Stat stat = zookeeper.setData(LEDGER_CURSOR_PREFIX + this.ledgerManager.getName() + "/" + name, JsonUtils.toJson(readPosition).getBytes(), currentVersion.getVersion());
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
        synchronized (this) {
            long entryId = readPosition.getEntryId() + numberToRead;
            readPosition = new Position(readPosition.getLedgerId(), entryId);
            logger.info("Current entryId {}." + entryId);
        }
        return wrappers;
    }

    @Override public void asyncReadEntries(int numberToRead, AsyncCallbacks.ReadEntryCallback callback) {
        this.ledgerManager.asyncReadEntries(numberToRead, readPosition, callback);
        synchronized (this) {
            long entryId = readPosition.getEntryId() + numberToRead;
            readPosition = new Position(readPosition.getLedgerId(), entryId);
            logger.info("Current entryId {}." + entryId);
        }
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
