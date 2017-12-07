package com.song.fastmq.broker.storage.impl;

import com.song.fastmq.broker.storage.LedgerCursor;
import com.song.fastmq.broker.storage.LedgerEntryWrapper;
import com.song.fastmq.broker.storage.LedgerStorageException;
import com.song.fastmq.broker.storage.Position;
import com.song.fastmq.broker.storage.Version;
import com.song.fastmq.broker.storage.concurrent.AsyncCallback;
import com.song.fastmq.broker.storage.concurrent.AsyncCallbacks;
import com.song.fastmq.broker.storage.config.BookKeeperConfig;
import com.song.fastmq.common.utils.JsonUtils;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.logging.log4j.core.config.Configurator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.assertEquals;

/**
 * Created by song on 下午10:02.
 */
public class LedgerManagerImplTest {

    private static Logger logger = LoggerFactory.getLogger(LedgerManagerImplTest.class);

    private LedgerManagerImpl ledgerManager;

    @Before
    public void setUp() throws Exception {
        Configurator.initialize("FastMQ", Thread.currentThread().getContextClassLoader(), "log4j2.xml");
        CountDownLatch initLatch = new CountDownLatch(1);
        CuratorFramework curatorFramework = CuratorFrameworkFactory.newClient("127.0.0.1:2181", new ExponentialBackoffRetry(1000, 3));
        curatorFramework.start();
        AsyncCuratorFramework asyncCuratorFramework = AsyncCuratorFramework.wrap(curatorFramework);
        ledgerManager = new LedgerManagerImpl("HelloWorldTest", new BookKeeperConfig(), new BookKeeper("127.0.0.1:2181"), asyncCuratorFramework, new LedgerManagerStorageImpl(asyncCuratorFramework));
        ledgerManager.init(new AsyncCallback<Void, LedgerStorageException>() {
            @Override public void onCompleted(Void data, Version version) {
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
        assertEquals("JustATest", ledgerManager.getName());
    }

    @Test(timeout = 3000)
    public void addEntry() throws Throwable {
        class Result {
            Position first;
            Position last;
            Throwable throwable;
        }
        Result result = new Result();
        int count = 100;
        AtomicInteger atomicInteger = new AtomicInteger();
        final CountDownLatch downLatch = new CountDownLatch(100);
        for (int i = 0; i < count; i++) {
            ledgerManager.asyncAddEntry("Hello World".getBytes(), new AsyncCallback<Position, LedgerStorageException>() {
                @Override public void onCompleted(Position data, Version version) {
                    if (result.first == null) {
                        result.first = data;
                    }
                    result.last = data;
                    downLatch.countDown();
                    atomicInteger.incrementAndGet();
                }

                @Override public void onThrowable(LedgerStorageException throwable) {
                    downLatch.countDown();
                    result.throwable = throwable;
                }
            });
        }
        downLatch.await();
        if (result.throwable != null) {
            throw result.throwable;
        }
        assertEquals(100, atomicInteger.get());
        assertEquals(100, result.last.getEntryId() - result.first.getEntryId() + 1);
    }

    @Test
    public void read() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Position> positionAtomicReference = new AtomicReference<>();
        ledgerManager.asyncAddEntry("Hello World".getBytes(), new AsyncCallback<Position, LedgerStorageException>() {
            @Override public void onCompleted(Position data, Version version) {
                positionAtomicReference.set(data);
                System.out.println(JsonUtils.toJsonQuietly(data));
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
                assertEquals("Hello World", new String(wrapper.getData()));
            });
        } catch (InterruptedException | LedgerStorageException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void openCursor() throws Throwable {
        int count = 100;
        for (int i = 0; i < count; i++) {
            ledgerManager.addEntry(("Hello World :" + i).getBytes());
        }
        class Result {
            LedgerCursor ledgerCursor;
            Throwable throwable;
        }
        Result result = new Result();
        CountDownLatch latch = new CountDownLatch(1);
        ledgerManager.asyncOpenCursor("test-reader", new AsyncCallbacks.OpenCursorCallback() {
            @Override public void onComplete(LedgerCursor ledgerCursor) {
                result.ledgerCursor = ledgerCursor;
                latch.countDown();
            }

            @Override public void onThrowable(Throwable throwable) {
                result.throwable = throwable;
                latch.countDown();
            }
        });
        latch.await();
        if (result.throwable != null) {
            throw result.throwable;
        }
        LedgerCursor ledgerCursor = result.ledgerCursor;
        CountDownLatch readLatch = new CountDownLatch(1);
        logger.info("Try to read entries");
        ledgerCursor.asyncReadEntries(count, new AsyncCallbacks.ReadEntryCallback() {
            @Override public void readEntryComplete(List<LedgerEntryWrapper> entries) {
                assertEquals(count, entries.size());
                readLatch.countDown();
            }

            @Override public void readEntryFailed(Throwable throwable) {
                throwable.printStackTrace();
                readLatch.countDown();
            }
        });
        readLatch.await();
        ledgerCursor.close();
    }

    @After
    public void tearDown() throws Exception {
        if (ledgerManager != null) {
            ledgerManager.close();
        }
    }
}