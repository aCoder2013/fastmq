package com.song.fastmq.storage.impl;

import com.jayway.jsonassert.JsonAssert;
import com.song.fastmq.common.utils.JsonUtils;
import com.song.fastmq.storage.LedgerInfo;
import com.song.fastmq.storage.LedgerInfoManager;
import com.song.fastmq.storage.LedgerManagerStorage;
import com.song.fastmq.storage.LedgerStorageException;
import com.song.fastmq.storage.Version;
import com.song.fastmq.storage.concurrent.AsyncCallback;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by song on 2017/11/5.
 */
public class LedgerManagerStorageImplTest {

    private ZooKeeper zookeeper;

    private LedgerManagerStorage ledgerManagerStorage;

    @Before
    public void setUp() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        zookeeper = new ZooKeeper("127.0.0.1:2181", 10000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                System.out.println("Zookeeper connected.");
            } else {
                throw new RuntimeException("Error connecting to zookeeper");
            }
            latch.countDown();
        });
        latch.await();
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient("127.0.0.1:2181", new ExponentialBackoffRetry(1000, 3));
        curatorFramework.start();
        AsyncCuratorFramework asyncCuratorFramework = AsyncCuratorFramework.wrap(curatorFramework);
        ledgerManagerStorage = new LedgerManagerStorageImpl(asyncCuratorFramework);
    }

    @Test
    public void getLedgerStream() throws Exception {
        String ledgerName = "HelloWorldTest1";
        LedgerInfoManager ledgerInfoManager = ledgerManagerStorage.getLedgerManager(ledgerName);
        String json = JsonUtils.get().writeValueAsString(ledgerInfoManager);
        JsonAssert.with(json).assertEquals("$.name", ledgerName);
    }

    @Test
    public void asyncUpdateLedgerStream() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger counter = new AtomicInteger();
        String ledgerName = "HelloWorldTest1";
        ledgerManagerStorage.asyncGetLedgerManager(ledgerName, new AsyncCallback<LedgerInfoManager, LedgerStorageException>() {

            @Override public void onCompleted(LedgerInfoManager data, Version version) {
                data.setLedgers(Collections.singletonList(new LedgerInfo()));
                ledgerManagerStorage.asyncUpdateLedgerManager(ledgerName, data, version, new AsyncCallback<Void, LedgerStorageException>() {

                    @Override public void onCompleted(Void data, Version version) {
                        counter.incrementAndGet();
                        latch.countDown();
                    }

                    @Override public void onThrowable(LedgerStorageException throwable) {
                        throwable.printStackTrace();
                        latch.countDown();
                    }
                });
            }

            @Override public void onThrowable(LedgerStorageException throwable) {
                throwable.printStackTrace();
                latch.countDown();
            }
        });
        latch.await();
        Assert.assertEquals(1, counter.get());
    }

    @Test
    public void asyncRemoveLedger() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger counter = new AtomicInteger();
        ledgerManagerStorage.asyncRemoveLedger("HelloWorldTest1", new AsyncCallback<Void, LedgerStorageException>() {
            @Override public void onCompleted(Void data, Version version) {
                counter.incrementAndGet();
                latch.countDown();
            }

            @Override public void onThrowable(LedgerStorageException throwable) {
                throwable.printStackTrace();
                latch.countDown();
            }
        });
        latch.await();
        Assert.assertEquals(1, counter.get());
    }

    @Test
    public void removeLedger() throws Exception {
        ledgerManagerStorage.removeLedger("HelloWorldTest1");
    }

    @After
    public void tearDown() throws Exception {
        zookeeper.close();
    }
}