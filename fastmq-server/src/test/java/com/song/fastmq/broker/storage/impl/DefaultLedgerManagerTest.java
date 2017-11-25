package com.song.fastmq.broker.storage.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.song.fastmq.broker.storage.AsyncCallback;
import com.song.fastmq.broker.storage.LedgerEntryWrapper;
import com.song.fastmq.broker.storage.LedgerStorageException;
import com.song.fastmq.broker.storage.Position;
import com.song.fastmq.broker.storage.Version;
import com.song.fastmq.broker.storage.config.BookKeeperConfig;
import com.song.fastmq.common.utils.JsonUtils;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Created by song on 下午10:02.
 */
public class DefaultLedgerManagerTest {

    private DefaultLedgerManager ledgerManager;

    @Before
    public void setUp() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        ZooKeeper zookeeper = new ZooKeeper("127.0.0.1:2181", 10000, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected) {
                System.out.println("Zookeeper connected.");
            } else {
                throw new RuntimeException("Error connecting to zookeeper");
            }
            latch.countDown();
        });
        latch.await();
        CountDownLatch initLatch = new CountDownLatch(1);
        ledgerManager = new DefaultLedgerManager("JustATest", new BookKeeperConfig(), new BookKeeper("127.0.0.1:2181"), new DefaultLedgerStreamStorage(zookeeper));
        ledgerManager.init(new AsyncCallback<Void, LedgerStorageException>() {
            @Override public void onCompleted(Void result, Version version) {
                initLatch.countDown();
            }

            @Override public void onThrowable(LedgerStorageException throwable) {
                throwable.printStackTrace();
                initLatch.countDown();
            }
        });
        initLatch.await();
    }

    @Test
    public void getName() throws Exception {
        Assert.assertEquals("JustATest", ledgerManager.getName());
    }

    @Test(timeout = 3000)
    public void addEntry() throws Exception {
        int count = 100;
        AtomicInteger atomicInteger = new AtomicInteger();
        final CountDownLatch downLatch = new CountDownLatch(100);
        for (int i = 0; i < count; i++) {
            ledgerManager.asyncAddEntry("Hello World".getBytes(), new AsyncCallback<Position, LedgerStorageException>() {
                @Override public void onCompleted(Position result, Version version) {
                    try {
                        System.out.println(JsonUtils.get().writeValueAsString(result));
                    } catch (JsonProcessingException e) {
                        e.printStackTrace();
                    }
                    downLatch.countDown();
                    atomicInteger.incrementAndGet();
                }

                @Override public void onThrowable(LedgerStorageException throwable) {
                    throwable.printStackTrace();
                }
            });
        }
        downLatch.await();
        // TODO: 2017/11/19 make sure entry is actually stored into bookie
        Assert.assertEquals(100, atomicInteger.get());
    }

    @Test
    public void asyncAddEntry() throws Exception {

    }

    @Test
    public void read() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Position> positionAtomicReference = new AtomicReference<>();
        ledgerManager.asyncAddEntry("Hello World".getBytes(), new AsyncCallback<Position, LedgerStorageException>() {
            @Override public void onCompleted(Position result, Version version) {
                positionAtomicReference.set(result);
                System.out.println(JsonUtils.toJsonQuietly(result));
                latch.countDown();
            }

            @Override public void onThrowable(LedgerStorageException throwable) {
                throwable.printStackTrace();
                latch.countDown();
            }
        });
        latch.await();
        try {
            List<LedgerEntryWrapper> wrappers = ledgerManager.readEntries(1, positionAtomicReference.get());
            System.out.println(wrappers.size());
            wrappers.forEach(wrapper -> {
                System.out.println(new String(wrapper.getData()));
                Assert.assertEquals("Hello World", new String(wrapper.getData()));
            });
        } catch (InterruptedException | LedgerStorageException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void readEntryInsertedBefore() throws Exception {
        String json = "{\"ledgerId\":5,\"entryId\":0}\n";
        Position position = JsonUtils.fromJson(json, Position.class);
        List<LedgerEntryWrapper> wrappers = ledgerManager.readEntries(1, position);
        Assert.assertTrue(wrappers != null && wrappers.size() > 0);
        wrappers.forEach(wrapper -> {
            Assert.assertEquals("Hello World", new String(wrapper.getData()));
            System.out.println(JsonUtils.toJsonQuietly(wrapper));
        });
    }

    @After
    public void tearDown() throws Exception {
        ledgerManager.close();
    }
}