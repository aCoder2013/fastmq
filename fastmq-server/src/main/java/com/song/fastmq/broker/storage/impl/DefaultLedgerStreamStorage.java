package com.song.fastmq.broker.storage.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.song.fastmq.broker.storage.AsyncCallback;
import com.song.fastmq.broker.storage.BadVersionException;
import com.song.fastmq.broker.storage.CommonPool;
import com.song.fastmq.broker.storage.LedgerStorageException;
import com.song.fastmq.broker.storage.LedgerInfoManager;
import com.song.fastmq.broker.storage.LedgerStreamStorage;
import com.song.fastmq.broker.storage.Version;
import com.song.fastmq.common.utils.JsonUtils;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by song on 2017/11/5.
 */
public class DefaultLedgerStreamStorage implements LedgerStreamStorage {

    private static final Logger logger = LoggerFactory.getLogger(DefaultLedgerStreamStorage.class);

    private static final String LEDGER_NAME_PREFIX_NAME = "/fastmq/bk-ledgers";

    private static final String LEDGER_NAME_PREFIX = LEDGER_NAME_PREFIX_NAME + "/";

    private final ZooKeeper zooKeeper;

    public DefaultLedgerStreamStorage(ZooKeeper zooKeeper) throws Exception {
        this.zooKeeper = zooKeeper;
        if (zooKeeper.exists(LEDGER_NAME_PREFIX_NAME, false) == null) {
            ZkUtils.createFullPathOptimistic(zooKeeper, LEDGER_NAME_PREFIX_NAME, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    @Override
    public LedgerInfoManager getLedgerStream(String ledgerName) throws InterruptedException, LedgerStorageException {
        final LedgerResult ledgerResult = new LedgerResult();
        CountDownLatch latch = new CountDownLatch(1);
        asyncGetLedgerStream(ledgerName, new AsyncCallback<LedgerInfoManager, LedgerStorageException>() {

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

    @Override public void asyncGetLedgerStream(String name,
        AsyncCallback<LedgerInfoManager, LedgerStorageException> asyncCallback) {
        CommonPool.executeBlocking(() -> zooKeeper.getData(LEDGER_NAME_PREFIX + name, false, (rc, path, ctx, data, stat) -> {
            if (rc == KeeperException.Code.OK.intValue()) {
                try {
                    LedgerInfoManager ledgerInfoManager = JsonUtils.get().readValue(new String(data), LedgerInfoManager.class);
                    asyncCallback.onCompleted(ledgerInfoManager, new ZkVersion(stat.getVersion()));
                } catch (IOException e) {
                    asyncCallback.onThrowable(new LedgerStorageException(e));
                }
            } else if (rc == KeeperException.Code.NONODE.intValue()) {
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
                ZkUtils.asyncCreateFullPathOptimistic(zooKeeper, LEDGER_NAME_PREFIX + name, bytes,
                    ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, (rc1, path1, ctx1, name1) -> {
                        if (rc1 == KeeperException.Code.OK.intValue()) {
                            asyncCallback.onCompleted(ledgerInfoManager, new ZkVersion(0));
                        } else {
                            asyncCallback.onThrowable(new LedgerStorageException(KeeperException.create(KeeperException.Code.get(rc))));
                        }
                    }, null);
            } else {
                asyncCallback.onThrowable(new LedgerStorageException(KeeperException.create(KeeperException.Code.get(rc))));
            }
        }, null));
    }

    @Override public void asyncUpdateLedgerStream(String name, LedgerInfoManager ledgerInfoManager, Version version,
        AsyncCallback<Void, LedgerStorageException> asyncCallback) {
        CommonPool.executeBlocking(() -> {
            byte[] bytes;
            try {
                bytes = JsonUtils.get().writeValueAsBytes(ledgerInfoManager);
            } catch (JsonProcessingException e) {
                asyncCallback.onThrowable(new LedgerStorageException(e));
                return;
            }
            zooKeeper.setData(LEDGER_NAME_PREFIX + name, bytes, version.getVersion(), (rc, path, ctx, stat) -> {
                if (rc == KeeperException.Code.OK.intValue()) {
                    asyncCallback.onCompleted(null, new ZkVersion(stat.getVersion()));
                } else if (rc == KeeperException.Code.BADVERSION.intValue()) {
                    asyncCallback.onThrowable(new BadVersionException(KeeperException.create(KeeperException.Code.get(rc))));
                } else {
                    asyncCallback.onThrowable(new LedgerStorageException(KeeperException.create(KeeperException.Code.get(rc))));
                }
            }, null);
        });
    }

    @Override public void asyncRemoveLedger(String name, AsyncCallback<Void, LedgerStorageException> asyncCallback) {
        logger.info("Remove ledger [{}].", name);
        zooKeeper.delete(LEDGER_NAME_PREFIX + name, -1, (rc, path, ctx) -> {
            if (rc == KeeperException.Code.OK.intValue()) {
                asyncCallback.onCompleted(null, null);
            } else {
                asyncCallback.onThrowable(new LedgerStorageException(KeeperException.create(KeeperException.Code.get(rc))));
            }
        }, null);
    }

    class LedgerResult {
        LedgerInfoManager ledgerInfoManager;

        LedgerStorageException exception;
    }
}
