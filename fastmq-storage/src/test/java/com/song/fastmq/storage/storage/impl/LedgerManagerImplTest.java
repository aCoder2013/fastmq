package com.song.fastmq.storage.storage.impl;

import static org.junit.Assert.assertEquals;

import com.song.fastmq.storage.storage.LedgerCursor;
import com.song.fastmq.storage.storage.LedgerEntry;
import com.song.fastmq.storage.storage.LedgerStorageException;
import com.song.fastmq.storage.storage.Position;
import com.song.fastmq.storage.storage.Version;
import com.song.fastmq.storage.storage.concurrent.AsyncCallback;
import com.song.fastmq.storage.storage.concurrent.AsyncCallbacks;
import com.song.fastmq.storage.storage.config.BookKeeperConfig;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
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
        ledgerManager = new LedgerManagerImpl("ledger-manager-read-test-name", new BookKeeperConfig(), new BookKeeper("127.0.0.1:2181"), asyncCuratorFramework, new LedgerManagerStorageImpl(asyncCuratorFramework));
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
        int count = 213;
        AtomicInteger atomicInteger = new AtomicInteger();
        final CountDownLatch downLatch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            ledgerManager.asyncAddEntry(("Hello World" + i).getBytes(), new AsyncCallback<Position, LedgerStorageException>() {
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
        assertEquals(count, atomicInteger.get());
        assertEquals(count, result.last.getEntryId() - result.first.getEntryId() + 1);
    }

    @Test
    public void openCursor() throws Throwable {
        int count = 100;
        class Result {
            LedgerCursor ledgerCursor;
            Throwable throwable;
        }
        Result result = new Result();
        CountDownLatch latch = new CountDownLatch(1);
        ledgerManager.asyncOpenCursor("test-reader-1", new AsyncCallbacks.OpenCursorCallback() {
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
        ledgerCursor.asyncReadEntries(1000, new AsyncCallbacks.ReadEntryCallback() {
            @Override public void readEntryComplete(List<LedgerEntry> entries) {
                entries.forEach(wrapper -> System.out.println(new String(wrapper.getData())));
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